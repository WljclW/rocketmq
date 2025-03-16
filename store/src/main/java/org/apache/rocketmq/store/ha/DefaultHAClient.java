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

package org.apache.rocketmq.store.ha;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.store.DefaultMessageStore;

public class DefaultHAClient extends ServiceThread implements HAClient {

    /**
     * Report header buffer size. Schema: slaveMaxOffset. Format:
     *
     * <pre>
     * ┌───────────────────────────────────────────────┐
     * │                  slaveMaxOffset               │
     * │                    (8bytes)                   │
     * ├───────────────────────────────────────────────┤
     * │                                               │
     * │                  Report Header                │
     * </pre>
     * <p>
     */
    public static final int REPORT_HEADER_SIZE = 8;

    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024 * 4;
    private final AtomicReference<String> masterHaAddress = new AtomicReference<>();
    /*主服务器地址*/
    private final AtomicReference<String> masterAddress = new AtomicReference<>();
    /*从服务器向主服务器发起主从同步的拉取偏移量。*/
    private final ByteBuffer reportOffset = ByteBuffer.allocate(REPORT_HEADER_SIZE);
    /*网络传输通道*/
    private SocketChannel socketChannel;
    /*NIO事件选择器*/
    private Selector selector;
    /**
     * 上一次写入消息时的时间戳(从服务器写(从主服务器接收的)数据)
     * last time that slave reads date from master.
     */
    private long lastReadTimestamp = System.currentTimeMillis();
    /**
     * last time that slave reports offset to master.
     */
    private long lastWriteTimestamp = System.currentTimeMillis();
    /*反馈从服务器当前的复制进度，即CommitLog文件的最大偏移量。*/
    private long currentReportedOffset = 0;
    /*本次已处理读缓存区的指针*/
    private int dispatchPosition = 0;
    /*读缓冲区，大小是4M*/
    private ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
    /*读缓冲区备份，与byteBufferRead进行交换*/
    private ByteBuffer byteBufferBackup = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
    private DefaultMessageStore defaultMessageStore;
    private volatile HAConnectionState currentState = HAConnectionState.READY;
    private FlowMonitor flowMonitor;

    public DefaultHAClient(DefaultMessageStore defaultMessageStore) throws IOException {
        this.selector = NetworkUtil.openSelector();
        this.defaultMessageStore = defaultMessageStore;
        this.flowMonitor = new FlowMonitor(defaultMessageStore.getMessageStoreConfig());
    }

    public void updateHaMasterAddress(final String newAddr) {
        String currentAddr = this.masterHaAddress.get();
        if (masterHaAddress.compareAndSet(currentAddr, newAddr)) {
            log.info("update master ha address, OLD: " + currentAddr + " NEW: " + newAddr);
        }
    }

    public void updateMasterAddress(final String newAddr) {
        String currentAddr = this.masterAddress.get();
        if (masterAddress.compareAndSet(currentAddr, newAddr)) {
            log.info("update master address, OLD: " + currentAddr + " NEW: " + newAddr);
        }
    }

    public String getHaMasterAddress() {
        return this.masterHaAddress.get();
    }

    public String getMasterAddress() {
        return this.masterAddress.get();
    }

    /**[]:判断是否需要向主服务器反馈当前带拉取消息的偏移量
     * 【】：
     * 1. 主从服务器的高可用时间间隔默认是5秒，由参数haSendHeartbeatInterval确定*/
    private boolean isTimeToReportOffset() {
        long interval = defaultMessageStore.now() - this.lastWriteTimestamp;
        return interval > defaultMessageStore.getMessageStoreConfig().getHaSendHeartbeatInterval();
    }

    /**【】：从节点（Slave）向主节点（Master）报告其最大偏移量（maxOffset）的逻辑实现
     * 对于从服务器来说，是发送下次待拉取消息的偏移量；而对于主服务器来说，既可以认为是从服
     * 务器本次请求拉取的消息偏移量，也可以理解为从服务器的消息同步ACK确认消息
     * 【说明】
     *  RocketMQ提供了一个基于NIO的网络写示例程序：首先将ByteBuffer的position设置为0，limit设置为待写
     *      入字节长度；然后调用putLong将待拉取消息的偏移量写入ByteBuffer，需要将ByteBuffer从写模式切
     *      换到读模式，这里的做法是手动将position设置为0，limit设置为可读长度，其实也可以直接调
     *      用ByteBuffer的flip()方法来切换ByteBuffer的读写状态。特别需要留意的是，调用网络通道的write()
     *      方法是在一个while循环中反复判断byteBuffer是否全部写入通道中，这是由于NIO是一个非阻塞I/O，调
     *      用一次write()方法不一定能将ByteBuffer可读字节全部写入*/
    private boolean reportSlaveMaxOffset(final long maxOffset) {
        this.reportOffset.position(0);
        this.reportOffset.limit(REPORT_HEADER_SIZE);
        this.reportOffset.putLong(maxOffset);
        this.reportOffset.position(0);
        this.reportOffset.limit(REPORT_HEADER_SIZE);

        for (int i = 0; i < 3 && this.reportOffset.hasRemaining(); i++) {
            try {
                this.socketChannel.write(this.reportOffset);
            } catch (IOException e) {
                log.error(this.getServiceName()
                    + "reportSlaveMaxOffset this.socketChannel.write exception", e);
                return false;
            }
        }
        lastWriteTimestamp = this.defaultMessageStore.getSystemClock().now();
        return !this.reportOffset.hasRemaining();
    }

    private void reallocateByteBuffer() {
        int remain = READ_MAX_BUFFER_SIZE - this.dispatchPosition;
        if (remain > 0) {
            this.byteBufferRead.position(this.dispatchPosition);

            this.byteBufferBackup.position(0);
            this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);
            this.byteBufferBackup.put(this.byteBufferRead);
        }

        this.swapByteBuffer();

        this.byteBufferRead.position(remain);
        this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
        this.dispatchPosition = 0;
    }

    private void swapByteBuffer() {
        ByteBuffer tmp = this.byteBufferRead;
        this.byteBufferRead = this.byteBufferBackup;
        this.byteBufferBackup = tmp;
    }

    /**[]:从服务器读取 主服务器 发来的数据
     * 【】
     * 1. 处理网络读请求，即处理从主服务器传回的消息数据。RocketMQ给出了一个处理网络读请求的NIO示例。循环判断
     * readByteBuffer是否还有剩余空间，如果存在剩余空间，则调用SocketChannel#read（ByteBuffer readByteBuffer）方
     * 法，将通道中的数据读入读缓存区
     * */
    private boolean processReadEvent() {
        int readSizeZeroTimes = 0;
        while (this.byteBufferRead.hasRemaining()) {
            try {
                int readSize = this.socketChannel.read(this.byteBufferRead);
                if (readSize > 0) {
                    flowMonitor.addByteCountTransferred(readSize);
                    readSizeZeroTimes = 0;
                    /*调用dispatchReadRequest()将读取到的所有信息全部追加到消息内存映射文件中，再次反馈拉取
                    进度给主服务器*/
                    boolean result = this.dispatchReadRequest();
                    if (!result) {
                        log.error("HAClient, dispatchReadRequest error");
                        return false;
                    }
                    lastReadTimestamp = System.currentTimeMillis();
                } else if (readSize == 0) {
                    //如果连续3次从网络通道读取的字节数是0，则结束本次读任务
                    if (++readSizeZeroTimes >= 3) {
                        break;
                    }
                } else {
                    //读取到的字节数小于0，返回false
                    log.info("HAClient, processReadEvent read socket < 0");
                    return false;
                }
            } catch (IOException e) {
                log.info("HAClient, processReadEvent read socket exception", e);
                return false;
            }
        }

        return true;
    }

    private boolean dispatchReadRequest() {
        int readSocketPos = this.byteBufferRead.position();

        while (true) {
            int diff = this.byteBufferRead.position() - this.dispatchPosition;
            if (diff >= DefaultHAConnection.TRANSFER_HEADER_SIZE) {
                long masterPhyOffset = this.byteBufferRead.getLong(this.dispatchPosition);
                int bodySize = this.byteBufferRead.getInt(this.dispatchPosition + 8);

                long slavePhyOffset = this.defaultMessageStore.getMaxPhyOffset();

                if (slavePhyOffset != 0) {
                    if (slavePhyOffset != masterPhyOffset) {
                        log.error("master pushed offset not equal the max phy offset in slave, SLAVE: "
                            + slavePhyOffset + " MASTER: " + masterPhyOffset);
                        return false;
                    }
                }

                if (diff >= (DefaultHAConnection.TRANSFER_HEADER_SIZE + bodySize)) {
                    byte[] bodyData = byteBufferRead.array();
                    int dataStart = this.dispatchPosition + DefaultHAConnection.TRANSFER_HEADER_SIZE;

                    this.defaultMessageStore.appendToCommitLog(
                        masterPhyOffset, bodyData, dataStart, bodySize);

                    this.byteBufferRead.position(readSocketPos);
                    this.dispatchPosition += DefaultHAConnection.TRANSFER_HEADER_SIZE + bodySize;

                    if (!reportSlaveMaxOffsetPlus()) {
                        return false;
                    }

                    continue;
                }
            }

            if (!this.byteBufferRead.hasRemaining()) {
                this.reallocateByteBuffer();
            }

            break;
        }

        return true;
    }

    private boolean reportSlaveMaxOffsetPlus() {
        boolean result = true;
        long currentPhyOffset = this.defaultMessageStore.getMaxPhyOffset();
        if (currentPhyOffset > this.currentReportedOffset) {
            this.currentReportedOffset = currentPhyOffset;
            result = this.reportSlaveMaxOffset(this.currentReportedOffset);
            if (!result) {
                this.closeMaster();
                log.error("HAClient, reportSlaveMaxOffset error, " + this.currentReportedOffset);
            }
        }

        return result;
    }

    public void changeCurrentState(HAConnectionState currentState) {
        log.info("change state to {}", currentState);
        this.currentState = currentState;
    }

    /**[]:连接主服务器(主broker)。更新一些标志字段
     * 流程：从服务器连接主服务器。如果socketChannel为空，则尝试连接主服务器。如果主服务器地址为
     * 空，返回false。如果主服务器地址不为空，则建立到主服务器的TCP连接，然后注册OP_READ（网络
     * 读事件），初始化currentReportedOffset为CommitLog文件的最大偏移量、lastWriteTimestamp上
     * 次写入时间戳为当前时间戳，并返回true。
     * @return true：连接成功，false：连接主服务器失败*/
    public boolean connectMaster() throws ClosedChannelException {
        if (null == socketChannel) {
            String addr = this.masterHaAddress.get();
            /*if块：如果 主服务器 的地址不是空，则建立到主服务器的TCP连接(这里的连接指的是SocketChannel)，然
            后注册OP_READ事件*/
            if (addr != null) {
                SocketAddress socketAddress = NetworkUtil.string2SocketAddress(addr);
                this.socketChannel = RemotingHelper.connect(socketAddress);
                if (this.socketChannel != null) {
                    this.socketChannel.register(this.selector, SelectionKey.OP_READ);
                    log.info("HAClient connect to master {}", addr);
                    this.changeCurrentState(HAConnectionState.TRANSFER);
                }
            }
            /*初始化currentReportedOffset为CommitLog文件的最大偏移量*/
            this.currentReportedOffset = this.defaultMessageStore.getMaxPhyOffset();
            //更新lastReadTimestamp为当前时间戳
            this.lastReadTimestamp = System.currentTimeMillis();
        }

        return this.socketChannel != null;
    }

    public void closeMaster() {
        if (null != this.socketChannel) {
            try {

                SelectionKey sk = this.socketChannel.keyFor(this.selector);
                if (sk != null) {
                    sk.cancel();
                }

                this.socketChannel.close();

                this.socketChannel = null;

                log.info("HAClient close connection with master {}", this.masterHaAddress.get());
                this.changeCurrentState(HAConnectionState.READY);
            } catch (IOException e) {
                log.warn("closeMaster exception. ", e);
            }

            this.lastReadTimestamp = 0;
            this.dispatchPosition = 0;

            this.byteBufferBackup.position(0);
            this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);

            this.byteBufferRead.position(0);
            this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
        }
    }

    /**[]:在后台持续运行，用于处理从节点与主节点之间的连接、数据传输和状态管理。
     *
     * */
    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");
        /**第一步：启动FlowMonitor服务*/
        this.flowMonitor.start();

        while (!this.isStopped()) {
            try {
                switch (this.currentState) {
                    case SHUTDOWN:
                        this.flowMonitor.shutdown(true);
                        return;
                    case READY:
                        if (!this.connectMaster()) {
                            log.warn("HAClient connect to master {} failed", this.masterHaAddress.get());
                            this.waitForRunning(1000 * 5);
                        }
                        continue;
                    case TRANSFER:
                        if (!transferFromMaster()) {
                            closeMasterAndWait();
                            continue;
                        }
                        break;
                    default:
                        this.waitForRunning(1000 * 2);
                        continue;
                }
                long interval = this.defaultMessageStore.now() - this.lastReadTimestamp;
                if (interval > this.defaultMessageStore.getMessageStoreConfig().getHaHousekeepingInterval()) {
                    log.warn("AutoRecoverHAClient, housekeeping, found this connection[" + this.masterHaAddress
                        + "] expired, " + interval);
                    this.closeMaster();
                    log.warn("AutoRecoverHAClient, master not response some time, so close connection");
                }
            } catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
                this.closeMasterAndWait();
            }
        }

        this.flowMonitor.shutdown(true);
        log.info(this.getServiceName() + " service end");
    }

    private boolean transferFromMaster() throws IOException {
        boolean result;
        /*如果“到了向主节点汇报进度的事件，就向主服务器汇报进度”*/
        if (this.isTimeToReportOffset()) {
            log.info("Slave report current offset {}", this.currentReportedOffset);
            result = this.reportSlaveMaxOffset(this.currentReportedOffset);
            if (!result) { //汇报进度时出现异常，直接return false.
                return false;
            }
        }
        /*调用 selector.select(1000) 方法等待最多 1 秒钟，监听是否有可读事件到达。
        Selector 是 Java NIO 的核心组件，用于高效地监控多个通道的事件（如读、写等）。
        这里的作用是等待主节点发送的数据到达。
        * */
        this.selector.select(1000);
        //调用 processReadEvent() 方法处理可读事件————在这里指的就是主节点发过来的数据
        result = this.processReadEvent();
        if (!result) {
            return false;
        }
        //调用 reportSlaveMaxOffsetPlus() 方法再次向主节点报告从节点的最大偏移量。
        return reportSlaveMaxOffsetPlus();
    }

    public void closeMasterAndWait() {
        this.closeMaster();
        this.waitForRunning(1000 * 5);
    }

    public long getLastWriteTimestamp() {
        return this.lastWriteTimestamp;
    }

    public long getLastReadTimestamp() {
        return lastReadTimestamp;
    }

    @Override
    public HAConnectionState getCurrentState() {
        return currentState;
    }

    @Override
    public long getTransferredByteInSecond() {
        return flowMonitor.getTransferredByteInSecond();
    }

    @Override
    public void shutdown() {
        this.changeCurrentState(HAConnectionState.SHUTDOWN);
        this.flowMonitor.shutdown();
        super.shutdown();

        closeMaster();
        try {
            this.selector.close();
        } catch (IOException e) {
            log.warn("Close the selector of AutoRecoverHAClient error, ", e);
        }
    }

    @Override
    public String getServiceName() {
        if (this.defaultMessageStore != null && this.defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
            return this.defaultMessageStore.getBrokerIdentity().getIdentifier() + DefaultHAClient.class.getSimpleName();
        }
        return DefaultHAClient.class.getSimpleName();
    }
}
