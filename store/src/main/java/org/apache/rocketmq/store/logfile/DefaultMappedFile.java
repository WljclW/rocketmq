/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.logfile;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.commons.lang3.SystemUtils;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.AppendMessageCallback;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.CompactionAppendMsgCallback;
import org.apache.rocketmq.store.PutMessageContext;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.TransientStorePool;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.util.LibC;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

public class DefaultMappedFile extends AbstractMappedFile {
    public static final int OS_PAGE_SIZE = 1024 * 4; //操作系统每一页的大小，默认是4KB
    public static final Unsafe UNSAFE = getUnsafe();
    private static final Method IS_LOADED_METHOD;
    public static final int UNSAFE_PAGE_SIZE = UNSAFE == null ? OS_PAGE_SIZE : UNSAFE.pageSize();

    protected static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    protected static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);

    protected static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);

    /**
     * AtomicIntegerFieldUpdater:用于以原子方式更新“泛型对象”的int字段，在DefaultMappedFile的静态代码块中完成了字段的指定
     * */
    protected static final AtomicIntegerFieldUpdater<DefaultMappedFile> WROTE_POSITION_UPDATER;
    protected static final AtomicIntegerFieldUpdater<DefaultMappedFile> COMMITTED_POSITION_UPDATER;
    protected static final AtomicIntegerFieldUpdater<DefaultMappedFile> FLUSHED_POSITION_UPDATER;

    //上面的三个原子更新的字段，就是操作下面的三个volatile字段
    protected volatile int wrotePosition;
    protected volatile int committedPosition;
    protected volatile int flushedPosition;
    protected int fileSize;
    protected FileChannel fileChannel;
    /**
     * Message will put to here first, and then reput to FileChannel if writeBuffer is not null.
     * 堆外内存，如果启动堆外内存机制"transientStorePollEnable=true"，此字段不是空，消息会先被写入到这里，然后才会被写
     *      入到MappedFile创建的FileChannel中
     * 此字段的初始化是在init方法(构造器中会调用init)中，初始化时就是从transientStorePool中拿出一块ByteBuffer赋给writeBuffer字段
     */
    protected ByteBuffer writeBuffer = null;
    //堆外内存池，其中的内存会被锁定在内存,使用方法"LibC.INSTANCE.mlock"
    protected TransientStorePool transientStorePool = null;
    protected String fileName; //文件名，其实 内容上 就等价于 fileFromOffset
    protected long fileFromOffset;  //在init方法(在构造器中会被调用)中会初始化，该值就是文件名
    protected File file;
    /**
     * 物理文件对应的内存映射ByteBuffer。。NIO中的类
     * MappedByteBuffer是NIO中的类，表示内存映射文件的缓冲区，允许将文件全部或者一部分映射到内存中，通过操作内存来读写文件内容
     * */
    protected MappedByteBuffer mappedByteBuffer;
    protected volatile long storeTimestamp = 0; //最后一次写入内容的时间
    protected boolean firstCreateInQueue = false; //是否是队列中首次创建的文件？？
    private long lastFlushTime = -1L;

    protected MappedByteBuffer mappedByteBufferWaitToClean = null;
    protected long swapMapTime = 0L;
    protected long mappedByteBufferAccessCountSinceLastSwap = 0L;

    /**
     * If this mapped file belongs to consume queue, this field stores store-timestamp of first message referenced
     * by this logical queue.
     */
    private long startTimestamp = -1;

    /**
     * If this mapped file belongs to consume queue, this field stores store-timestamp of last message referenced
     * by this logical queue.
     */
    private long stopTimestamp = -1;

    static {
        //指定上面原子更新的字段，分别是那个字段
        WROTE_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "wrotePosition");
        COMMITTED_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "committedPosition");
        FLUSHED_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "flushedPosition");
        //isLoaded0(下面的代码就涉及)是一个本地方法，用于检查给定地址范围内的内存是否已加载。它接收三个参数：内存地址、长度和页数，并返回一个布尔值表示是否加载成功。
        Method isLoaded0method = null;
        // On the windows platform and openjdk 11 method isLoaded0 always returns false.
        // see https://github.com/AdoptOpenJDK/openjdk-jdk11/blob/19fb8f93c59dfd791f62d41f332db9e306bc1422/src/java.base/windows/native/libnio/MappedByteBuffer.c#L34
        if (!SystemUtils.IS_OS_WINDOWS) {
            try {
                isLoaded0method = MappedByteBuffer.class.getDeclaredMethod("isLoaded0", long.class, long.class, int.class);
                isLoaded0method.setAccessible(true);
            } catch (NoSuchMethodException ignore) {
            }
        }
        IS_LOADED_METHOD = isLoaded0method;
    }

    public DefaultMappedFile() {
    }

    public DefaultMappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    public DefaultMappedFile(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    @Override
    public void init(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize);
        this.writeBuffer = transientStorePool.borrowBuffer();
        this.transientStorePool = transientStorePool;
    }

    private void init(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;

        UtilAll.ensureDirOK(this.file.getParent());

        try {
            //通过RandomAccessFile创建读写文件的通道
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            //创建内存映射文件
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);    //记录总的内存映射文件所占空间
            TOTAL_MAPPED_FILES.incrementAndGet();   //记录文件数量
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file " + this.fileName, e);
            throw e;
        } catch (IOException e) {
            log.error("Failed to map file " + this.fileName, e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    @Override
    public boolean renameTo(String fileName) {
        File newFile = new File(fileName);
        boolean rename = file.renameTo(newFile);
        if (rename) {
            this.fileName = fileName;
            this.file = newFile;
        }
        return rename;
    }

    @Override
    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public boolean getData(int pos, int size, ByteBuffer byteBuffer) {
        if (byteBuffer.remaining() < size) {
            return false;
        }

        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {

            if (this.hold()) {
                try {
                    int readNum = fileChannel.read(byteBuffer, pos);
                    return size == readNum;
                } catch (Throwable t) {
                    log.warn("Get data failed pos:{} size:{} fileFromOffset:{}", pos, size, this.fileFromOffset);
                    return false;
                } finally {
                    this.release();
                }
            } else {
                log.debug("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                    + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                + ", fileFromOffset: " + this.fileFromOffset);
        }

        return false;
    }

    @Override
    public int getFileSize() {
        return fileSize;
    }

    @Override
    public FileChannel getFileChannel() {
        return fileChannel;
    }

    public AppendMessageResult appendMessage(final ByteBuffer byteBufferMsg, final CompactionAppendMsgCallback cb) {
        assert byteBufferMsg != null;
        assert cb != null;

        int currentPos = WROTE_POSITION_UPDATER.get(this);
        if (currentPos < this.fileSize) {
            ByteBuffer byteBuffer = appendMessageBuffer().slice();
            byteBuffer.position(currentPos);
            AppendMessageResult result = cb.doAppend(byteBuffer, this.fileFromOffset, this.fileSize - currentPos, byteBufferMsg);
            WROTE_POSITION_UPDATER.addAndGet(this, result.getWroteBytes());
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    @Override
    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb,
        PutMessageContext putMessageContext) {
        return appendMessagesInner(msg, cb, putMessageContext);
    }

    @Override
    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb,
        PutMessageContext putMessageContext) {
        return appendMessagesInner(messageExtBatch, cb, putMessageContext);
    }

    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb,
        PutMessageContext putMessageContext) {
        assert messageExt != null;
        assert cb != null;

        int currentPos = WROTE_POSITION_UPDATER.get(this);

        if (currentPos < this.fileSize) { //不满足的化说明文件已经写满了，会抛出后文的UNKNOWN_ERROR异常
            /**
             * slice方法是创建一个ByteBuffer的视图，而不是创建一个ByteBuffer的副本。
             * 本质上是创建一个与原ByteBuffer共享的内存区，且拥有独立的position、limit、capacity
             * */
            ByteBuffer byteBuffer = appendMessageBuffer().slice();
            byteBuffer.position(currentPos);
            AppendMessageResult result;
            if (messageExt instanceof MessageExtBatch && !((MessageExtBatch) messageExt).isInnerBatch()) {
                // traditional batch message
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos,
                    (MessageExtBatch) messageExt, putMessageContext);
            } else if (messageExt instanceof MessageExtBrokerInner) {
                // traditional single message or newly introduced inner-batch message
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos,
                    (MessageExtBrokerInner) messageExt, putMessageContext);
            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }
            WROTE_POSITION_UPDATER.addAndGet(this, result.getWroteBytes());
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    protected ByteBuffer appendMessageBuffer() {
        this.mappedByteBufferAccessCountSinceLastSwap++;
        return writeBuffer != null ? writeBuffer : this.mappedByteBuffer;
    }

    @Override
    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    @Override
    public boolean appendMessage(final byte[] data) {
        return appendMessage(data, 0, data.length);
    }

    @Override
    public boolean appendMessage(ByteBuffer data) {
        int currentPos = WROTE_POSITION_UPDATER.get(this);
        int remaining = data.remaining();

        if ((currentPos + remaining) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                while (data.hasRemaining()) {
                    this.fileChannel.write(data);
                }
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            WROTE_POSITION_UPDATER.addAndGet(this, remaining);
            return true;
        }
        return false;
    }

    /**
     * Content of data from offset to offset + length will be written to file.
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    @Override
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = WROTE_POSITION_UPDATER.get(this);

        if ((currentPos + length) <= this.fileSize) {
            try {
                ByteBuffer buf = this.mappedByteBuffer.slice();
                buf.position(currentPos);
                buf.put(data, offset, length);
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            WROTE_POSITION_UPDATER.addAndGet(this, length);
            return true;
        }

        return false;
    }

    /**
     * @return The current flushed position
     */
    @Override
    public int flush(final int flushLeastPages) {
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {
                int value = getReadPosition();

                try {
                    this.mappedByteBufferAccessCountSinceLastSwap++;

                    /**
                     * 下面if-else的逻辑不是很懂？？？？
                     * 涉及到两种写入方式：
                     *      直接写入内存映射文件 ：数据直接写入 MappedByteBuffer，然后通过 mappedByteBuffer.force() 刷盘。
                     *      通过缓冲区写入 ：数据先写入 writeBuffer 或其他临时缓冲区，然后再提交到 fileChannel，最后通
                     *          过 fileChannel.force(false) 刷盘。
                     * */
                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        this.fileChannel.force(false);
                    } else {
                        /**
                         * force()方法可以确保自从上次调用force以后，所有对此ByteBuffer的更新都持久化到本地磁盘(如果链接的存储设备不
                         * 在本地，就不保证)
                         * */
                        this.mappedByteBuffer.force();
                    }
                    this.lastFlushTime = System.currentTimeMillis();
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }

                FLUSHED_POSITION_UPDATER.set(this, value);
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + FLUSHED_POSITION_UPDATER.get(this));
                FLUSHED_POSITION_UPDATER.set(this, getReadPosition());
            }
        }
        return this.getFlushedPosition(); //返回当前已经刷盘的位置
    }

    @Override
    public int commit(final int commitLeastPages) {
        if (writeBuffer == null) {  //没有必要执行提交
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            return WROTE_POSITION_UPDATER.get(this);
        }

        //no need to commit data to file channel, so just set committedPosition to wrotePosition.
        if (transientStorePool != null && !transientStorePool.isRealCommit()) {
            COMMITTED_POSITION_UPDATER.set(this, WROTE_POSITION_UPDATER.get(this));
        } else if (this.isAbleToCommit(commitLeastPages)) { //判断一下是不是需要执行commit
            //下面是真正执行提交
            if (this.hold()) {
                commit0();
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + COMMITTED_POSITION_UPDATER.get(this));
            }
        }

        /**
         * 下面的逻辑不是很懂？？？？if条件的判断含义是什么
         * */
        // All dirty data has been committed to FileChannel.(到了这里所有的脏数据就已经写到channel了)
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == COMMITTED_POSITION_UPDATER.get(this)) {
            this.transientStorePool.returnBuffer(writeBuffer);
            this.writeBuffer = null;
        }

        return COMMITTED_POSITION_UPDATER.get(this);
    }

    /**
     * 【总述】将数据提交到文件通道，负责将写缓冲区中尚未提交的数据写入文件通道
     * 【slice()】用于创建一个和原始ByteBuffer共享一片内存空间的ByteBuffer1，因此修改会互相影响；
     *           但是维护一套独立的position和limit指针(也就是说内容是相同的位置，但是由于通过ByteBuffer1向channel中写数据，因此
     *              它必须有独立的指针来标识写入位置、limit等)
     * */
    protected void commit0() {
        int writePos = WROTE_POSITION_UPDATER.get(this); //获取当前写入位置
        int lastCommittedPosition = COMMITTED_POSITION_UPDATER.get(this); //获取上次提交的位置

        if (writePos - lastCommittedPosition > 0) {
            try {
                //slice方法用于创建一个 和 当前ByteBuffer共享一片内存空间的ByteBuffer。因此修改会互相影响
                ByteBuffer byteBuffer = writeBuffer.slice();
                byteBuffer.position(lastCommittedPosition); //将新的byteBufer的position设置为上次提交的位置(lastCommitPosition)
                byteBuffer.limit(writePos); //将新的byteBufer的limit设置为当前写入位置(writePos)，也就是最大的可以拿数据的位置
                this.fileChannel.position(lastCommittedPosition); //设置文件通道的当前位置为上次提交的位置
                this.fileChannel.write(byteBuffer); //将ByteBuffer中的数据写入文件通道
                COMMITTED_POSITION_UPDATER.set(this, writePos);
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    //判断一下是不是需要flush了，要求能flush的页面不少于型参数值。。。方法逻辑类似于isAbleToCommit()
    private boolean isAbleToFlush(final int flushLeastPages) {
        int flush = FLUSHED_POSITION_UPDATER.get(this);
        int write = getReadPosition();

        if (this.isFull()) {
            return true;
        }

        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }
        //如果mappedfile没有满 并且 flushLeastPages <= 0，就会返回是不是有新数据
        return write > flush; //write>flush其实就是有新的没有落盘的数据
    }

    //判断是否可以提交文件
    protected boolean isAbleToCommit(final int commitLeastPages) {
        //获取当前的 写入位置 和 提交位置
        int commit = COMMITTED_POSITION_UPDATER.get(this);
        int write = WROTE_POSITION_UPDATER.get(this);

        if (this.isFull()) { //isFull()判断是否文件已写满
            return true;
        }
        //判断一下脏数据的位置是不是至少有commitLeastPages页
        if (commitLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (commit / OS_PAGE_SIZE)) >= commitLeastPages;
        }
        //文件没有被写满 并且 commitLeastPages <= 0，就会返回 下面的值
        return write > commit; //实质就是表示 ByteBuffer是不是有脏数据
    }

    @Override
    public int getFlushedPosition() {
        return FLUSHED_POSITION_UPDATER.get(this);
    }

    @Override
    public void setFlushedPosition(int pos) {
        FLUSHED_POSITION_UPDATER.set(this, pos);
    }

    @Override
    public boolean isFull() {
        return this.fileSize == WROTE_POSITION_UPDATER.get(this);
    }

    @Override
    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) { //判断从当前位置读取size会不会超过有效位置，如果没有的话就会去取消息
            if (this.hold()) {
                this.mappedByteBufferAccessCountSinceLastSwap++;

                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                    + this.fileFromOffset);
            }
        } else { //如果超过了有效位置，则返回null
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

    @Override
    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        // 获取这个MappedFile 里面的一个read position，其实就是这个MappedFile 的一个可读位置
        int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                this.mappedByteBufferAccessCountSinceLastSwap++;
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = readPosition - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }

    @Override
    public boolean cleanup(final long currentRef) {
        if (this.isAvailable()) {  //为true时表示当前的mappedfile可用，不能清理
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have not shutdown, stop unmapping.");
            return false;
        }

        if (this.isCleanupOver()) { //检查文件是否已经清理过
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have cleanup, do not do it again.");
            return true;
        }
        //清理掉mappedByteBuffer，并更新一些相关属性
        UtilAll.cleanBuffer(this.mappedByteBuffer);
        UtilAll.cleanBuffer(this.mappedByteBufferWaitToClean);
        this.mappedByteBufferWaitToClean = null;
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    @Override
    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {
            try {
                long lastModified = getLastModifiedTimestamp();
                this.fileChannel.close();   //关闭文件通道
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete(); //删除物理文件
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                    + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                    + this.getFlushedPosition() + ", "
                    + UtilAll.computeElapsedTimeMilliseconds(beginTime)
                    + "," + (System.currentTimeMillis() - lastModified));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    @Override
    public int getWrotePosition() {
        return WROTE_POSITION_UPDATER.get(this);
    }

    @Override
    public void setWrotePosition(int pos) {
        WROTE_POSITION_UPDATER.set(this, pos);
    }

    /**
     * @return The max position which have valid data。。返回 有效消息 的最大位置
     * 1. transientStorePool == null
     *      transientStorePool 是一个临时存储池，用于在内存中缓存消息，然后再批量写入到磁盘文件中。如果 transientStorePool 为 null，表
     *      示没有使用临时存储池，所有写入操作直接写入到映射文件（mappedByteBuffer）中。
     * 2. !transientStorePool.isRealCommit()
     *      isRealCommit() 方法返回一个布尔值，表示是否需要真正地将数据提交到文件通道（fileChannel）。如果 isRealCommit() 返回 false，
     *      则表示不需要真正提交数据到文件通道，而是可以继续使用内存中的缓冲区。
     * 综合解释
     *      条件作用：当 transientStorePool == null || !transientStorePool.isRealCommit() 成立时，意味着：
     *          没有使用临时存储池 (transientStorePool == null)，或者使用了临时存储池但不需要真正提交数据到文件通
     *          道 (!transientStorePool.isRealCommit())在这种情况下，WROTE_POSITION_UPDATER.get(this)方法会返回当前的写入位
     *          置（wrotePosition），即认为所有写入的数据都是有效的，可以直接读取。
     *      反之：如果 transientStorePool != null && transientStorePool.isRealCommit()，则表示使用了临时存储池并
     *          且需要真正提交数据到文件通道。此时，COMMITTED_POSITION_UPDATER.get(this) 方法会返回已提交的位置（committedPosition），以确保
     *          只读取已经持久化到磁盘的数据。
     */
    @Override
    public int getReadPosition() {
        return transientStorePool == null || !transientStorePool.isRealCommit() ? WROTE_POSITION_UPDATER.get(this) : COMMITTED_POSITION_UPDATER.get(this);
    }

    @Override
    public void setCommittedPosition(int pos) {
        COMMITTED_POSITION_UPDATER.set(this, pos);
    }

    @Override
    public void warmMappedFile(FlushDiskType type, int pages) {
        this.mappedByteBufferAccessCountSinceLastSwap++;

        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        long flush = 0;
        // long time = System.currentTimeMillis();
        for (long i = 0, j = 0; i < this.fileSize; i += DefaultMappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put((int) i, (byte) 0);
            // force flush when flush disk type is sync
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            // if (j % 1000 == 0) {
            //     log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
            //     time = System.currentTimeMillis();
            //     try {
            //         Thread.sleep(0);
            //     } catch (InterruptedException e) {
            //         log.error("Interrupted", e);
            //     }
            // }
        }

        // force flush when prepare load finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
            System.currentTimeMillis() - beginTime);

        this.mlock();
    }

    @Override
    public boolean swapMap() {
        if (getRefCount() == 1 && this.mappedByteBufferWaitToClean == null) {

            if (!hold()) {
                log.warn("in swapMap, hold failed, fileName: " + this.fileName);
                return false;
            }
            try {
                this.mappedByteBufferWaitToClean = this.mappedByteBuffer;
                this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
                this.mappedByteBufferAccessCountSinceLastSwap = 0L;
                this.swapMapTime = System.currentTimeMillis();
                log.info("swap file " + this.fileName + " success.");
                return true;
            } catch (Exception e) {
                log.error("swapMap file " + this.fileName + " Failed. ", e);
            } finally {
                this.release();
            }
        } else {
            log.info("Will not swap file: " + this.fileName + ", ref=" + getRefCount());
        }
        return false;
    }

    @Override
    public void cleanSwapedMap(boolean force) {
        try {
            if (this.mappedByteBufferWaitToClean == null) {
                return;
            }
            long minGapTime = 120 * 1000L;
            long gapTime = System.currentTimeMillis() - this.swapMapTime;
            if (!force && gapTime < minGapTime) {
                Thread.sleep(minGapTime - gapTime);
            }
            UtilAll.cleanBuffer(this.mappedByteBufferWaitToClean);
            mappedByteBufferWaitToClean = null;
            log.info("cleanSwapedMap file " + this.fileName + " success.");
        } catch (Exception e) {
            log.error("cleanSwapedMap file " + this.fileName + " Failed. ", e);
        }
    }

    @Override
    public long getRecentSwapMapTime() {
        return 0;
    }

    @Override
    public long getMappedByteBufferAccessCountSinceLastSwap() {
        return this.mappedByteBufferAccessCountSinceLastSwap;
    }

    @Override
    public long getLastFlushTime() {
        return this.lastFlushTime;
    }

    @Override
    public String getFileName() {
        return fileName;
    }

    @Override
    public MappedByteBuffer getMappedByteBuffer() {
        this.mappedByteBufferAccessCountSinceLastSwap++;
        return mappedByteBuffer;
    }

    @Override
    public ByteBuffer sliceByteBuffer() {
        this.mappedByteBufferAccessCountSinceLastSwap++;
        return this.mappedByteBuffer.slice();
    }

    @Override
    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    @Override
    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    @Override
    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    @Override
    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    @Override
    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    @Override
    public File getFile() {
        return this.file;
    }

    @Override
    public void renameToDelete() {
        //use Files.move
        if (!fileName.endsWith(".delete")) {
            String newFileName = this.fileName + ".delete";
            try {
                Path newFilePath = Paths.get(newFileName);
                // https://bugs.openjdk.org/browse/JDK-4724038
                // https://bugs.java.com/bugdatabase/view_bug.do?bug_id=4715154
                // Windows can't move the file when mmapped.
                if (NetworkUtil.isWindowsPlatform() && mappedByteBuffer != null) {
                    long position = this.fileChannel.position();
                    UtilAll.cleanBuffer(this.mappedByteBuffer);
                    this.fileChannel.close();
                    Files.move(Paths.get(fileName), newFilePath, StandardCopyOption.ATOMIC_MOVE);
                    try (RandomAccessFile file = new RandomAccessFile(newFileName, "rw")) {
                        this.fileChannel = file.getChannel();
                        this.fileChannel.position(position);
                        this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
                    }
                } else {
                    Files.move(Paths.get(fileName), newFilePath, StandardCopyOption.ATOMIC_MOVE);
                }
                this.fileName = newFileName;
                this.file = new File(newFileName);
            } catch (IOException e) {
                log.error("move file {} failed", fileName, e);
            }
        }
    }

    @Override
    public void moveToParent() throws IOException {
        Path currentPath = Paths.get(fileName);
        String baseName = currentPath.getFileName().toString();
        Path parentPath = currentPath.getParent().getParent().resolve(baseName);
        // https://bugs.openjdk.org/browse/JDK-4724038
        // https://bugs.java.com/bugdatabase/view_bug.do?bug_id=4715154
        // Windows can't move the file when mmapped.
        if (NetworkUtil.isWindowsPlatform() && mappedByteBuffer != null) {
            long position = this.fileChannel.position();
            UtilAll.cleanBuffer(this.mappedByteBuffer);
            this.fileChannel.close();
            Files.move(Paths.get(fileName), parentPath, StandardCopyOption.ATOMIC_MOVE);
            try (RandomAccessFile file = new RandomAccessFile(parentPath.toFile(), "rw")) {
                this.fileChannel = file.getChannel();
                this.fileChannel.position(position);
                this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            }
        } else {
            Files.move(Paths.get(fileName), parentPath, StandardCopyOption.ATOMIC_MOVE);
        }
        this.file = parentPath.toFile();
        this.fileName = parentPath.toString();
    }

    @Override
    public String toString() {
        return this.fileName;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(long startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    public long getStopTimestamp() {
        return stopTimestamp;
    }

    public void setStopTimestamp(long stopTimestamp) {
        this.stopTimestamp = stopTimestamp;
    }


    public Iterator<SelectMappedBufferResult> iterator(int startPos) {
        return new Itr(startPos);
    }

    public static Unsafe getUnsafe() {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return (Unsafe) f.get(null);
        } catch (Exception ignore) {

        }
        return null;
    }

    public static long mappingAddr(long addr) {
        long offset = addr % UNSAFE_PAGE_SIZE;
        offset = (offset >= 0) ? offset : (UNSAFE_PAGE_SIZE + offset);
        return addr - offset;
    }

    public static int pageCount(long size) {
        return (int) (size + (long) UNSAFE_PAGE_SIZE - 1L) / UNSAFE_PAGE_SIZE;
    }

    @Override
    public boolean isLoaded(long position, int size) {
        if (IS_LOADED_METHOD == null) {
            return true;
        }
        try {
            long addr = ((DirectBuffer) mappedByteBuffer).address() + position;
            return (boolean) IS_LOADED_METHOD.invoke(mappedByteBuffer, mappingAddr(addr), size, pageCount(size));
        } catch (Exception e) {
            log.info("invoke isLoaded0 of file {} error:", file.getAbsolutePath(), e);
        }
        return true;
    }

    private class Itr implements Iterator<SelectMappedBufferResult> {
        private int start;
        private int current;
        private ByteBuffer buf;

        public Itr(int pos) {
            this.start = pos;
            this.current = pos;
            this.buf = mappedByteBuffer.slice();
            this.buf.position(start);
        }

        @Override
        public boolean hasNext() {
            return current < getReadPosition();
        }

        @Override
        public SelectMappedBufferResult next() {
            int readPosition = getReadPosition();
            if (current < readPosition && current >= 0) {
                if (hold()) {
                    ByteBuffer byteBuffer = buf.slice();
                    byteBuffer.position(current);
                    int size = byteBuffer.getInt(current);
                    ByteBuffer bufferResult = byteBuffer.slice();
                    bufferResult.limit(size);
                    current += size;
                    return new SelectMappedBufferResult(fileFromOffset + current, bufferResult, size,
                        DefaultMappedFile.this);
                }
            }
            return null;
        }

        @Override
        public void forEachRemaining(Consumer<? super SelectMappedBufferResult> action) {
            Iterator.super.forEachRemaining(action);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

}
