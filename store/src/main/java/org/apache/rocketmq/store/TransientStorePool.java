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
package org.apache.rocketmq.store;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

/**
 * 作用是管理一组直接字节缓冲区（ByteBuffer），这些缓冲区用于临时存储数据，通常是为了提高读写性能。
 * 【池化机制】
 * */
public class TransientStorePool {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final int poolSize; //池子中缓冲区的数量(可在broker配置文件中通过transient StorePoolSize进行设置，默认为5)
    //fileSize默认是mapedFileSizeCommitLog的大小，表明TransientStorePool为CommitLog文件服务
    private final int fileSize; //实例化池子的时候，池子内每个缓冲区(ByteBuffer)的大小。。rocketmq中的默认是1GB
    private final Deque<ByteBuffer> availableBuffers;  //池子中可用的ByteBuffer(缓冲区)。。刚开始创建后会把所有的缓冲区添加到这里，每次用的时候拿出一个
    private volatile boolean isRealCommit = true;

    //poolSize标识初始时 池子 的大小，fileSize标识每一块ByteBuffer的大小(就是每个CommitLog文件的大小，默认是1GB)
    public TransientStorePool(final int poolSize, final int fileSize) {
        this.poolSize = poolSize;
        this.fileSize = fileSize;
        this.availableBuffers = new ConcurrentLinkedDeque<>();
    }

    /**
     * It's a heavy init method.
     * 循环创建poolSize个ByteBuffer；将创建的ByteBuffer锁定到内存中；最后把创建的ByteBuffer(缓冲区)加入
     *      到availableBuffers中
     */
    public void init() {
        for (int i = 0; i < poolSize; i++) {   //循环创建poolSize个ByteBuffer
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(fileSize);
            //拿到创建的ByteBuffer(缓冲区)，并用pointer指针进行记录，最后用mlock锁定到内存
            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            //将缓冲区锁定到内存中，保证内存不会被置换到交换区
            LibC.INSTANCE.mlock(pointer, new NativeLong(fileSize));

            availableBuffers.offer(byteBuffer); //将创建的ByteBuffer(缓冲区)加入到availableBuffers中
        }
    }

    public void destroy() {
        for (ByteBuffer byteBuffer : availableBuffers) {
            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            LibC.INSTANCE.munlock(pointer, new NativeLong(fileSize));
        }
    }

    //初始化ByteBuffer的一些属性，然后将ByteBuffer加入到availableBuffers中(后续可以拿到然后填充数据)
    public void returnBuffer(ByteBuffer byteBuffer) {
        byteBuffer.position(0);
        byteBuffer.limit(fileSize);
        this.availableBuffers.offerFirst(byteBuffer);
    }

    //从池子里面拿出来一块ByteBuffer(缓冲区)，剩余的不超过40%时会打印警告日志
    public ByteBuffer borrowBuffer() {
        //从availableBuffers中取出一个ByteBuffer(缓冲区)，默认这一个就是1GB
        ByteBuffer buffer = availableBuffers.pollFirst();
        if (availableBuffers.size() < poolSize * 0.4) { //如果availableBuffers中还剩40%的缓冲区，就打印warn日志
            log.warn("TransientStorePool only remain {} sheets.", availableBuffers.size());
        }
        return buffer;
    }

    public int availableBufferNums() {
        return availableBuffers.size();
    }

    //判断数据是不是已经提交到了MappedFile中
    public boolean isRealCommit() {
        return isRealCommit;
    }

    public void setRealCommit(boolean realCommit) {
        isRealCommit = realCommit;
    }
}
