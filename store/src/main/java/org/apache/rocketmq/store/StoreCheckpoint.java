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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.logfile.DefaultMappedFile;

public class StoreCheckpoint {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final RandomAccessFile randomAccessFile;
    private final FileChannel fileChannel;
    private final MappedByteBuffer mappedByteBuffer;
    private volatile long physicMsgTimestamp = 0;
    private volatile long logicsMsgTimestamp = 0;
    private volatile long indexMsgTimestamp = 0;
    private volatile long masterFlushedOffset = 0;
    private volatile long confirmPhyOffset = 0;

    /**用于初始化检查点文件（Checkpoint File）。检查点文件记录了 Broker 的运行状态信息（如消息的时间戳、刷盘偏移量等），在系统恢复
     * 时可以利用这些信息快速定位最新的数据位置。*/
    public StoreCheckpoint(final String scpPath) throws IOException {
        /*创建 或者 加载检查点文件*/
        File file = new File(scpPath);
        UtilAll.ensureDirOK(file.getParent());
        boolean fileExists = file.exists();
        /*打开检查点文件，获取channel，并将内容映射到内存。每一块大小为OS的page size*/
        this.randomAccessFile = new RandomAccessFile(file, "rw");
        this.fileChannel = this.randomAccessFile.getChannel();
        this.mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, 0, DefaultMappedFile.OS_PAGE_SIZE);
        /*如果原来文件就存在，则加载文件中的信息*/
        if (fileExists) {
            log.info("store checkpoint file exists, " + scpPath);
            this.physicMsgTimestamp = this.mappedByteBuffer.getLong(0); //物理偏移量
            this.logicsMsgTimestamp = this.mappedByteBuffer.getLong(8); //逻辑偏移量
            this.indexMsgTimestamp = this.mappedByteBuffer.getLong(16); //index消息的时间戳
            this.masterFlushedOffset = this.mappedByteBuffer.getLong(24); //主节点刷盘偏移量
            this.confirmPhyOffset = this.mappedByteBuffer.getLong(32); //被确认的消息物理偏移量

            log.info("store checkpoint file physicMsgTimestamp " + this.physicMsgTimestamp + ", "
                + UtilAll.timeMillisToHumanString(this.physicMsgTimestamp));
            log.info("store checkpoint file logicsMsgTimestamp " + this.logicsMsgTimestamp + ", "
                + UtilAll.timeMillisToHumanString(this.logicsMsgTimestamp));
            log.info("store checkpoint file indexMsgTimestamp " + this.indexMsgTimestamp + ", "
                + UtilAll.timeMillisToHumanString(this.indexMsgTimestamp));
            log.info("store checkpoint file masterFlushedOffset " + this.masterFlushedOffset);
            log.info("store checkpoint file confirmPhyOffset " + this.confirmPhyOffset);
        } else { //文件不存在，记录日志表示是一个新的检查点文件
            log.info("store checkpoint file not exists, " + scpPath);
        }
    }

    public void shutdown() {
        this.flush();

        // unmap mappedByteBuffer
        UtilAll.cleanBuffer(this.mappedByteBuffer);

        try {
            this.fileChannel.close();
        } catch (IOException e) {
            log.error("Failed to properly close the channel", e);
        }
    }

    public void flush() {
        this.mappedByteBuffer.putLong(0, this.physicMsgTimestamp);
        this.mappedByteBuffer.putLong(8, this.logicsMsgTimestamp);
        this.mappedByteBuffer.putLong(16, this.indexMsgTimestamp);
        this.mappedByteBuffer.putLong(24, this.masterFlushedOffset);
        this.mappedByteBuffer.putLong(32, this.confirmPhyOffset);
        this.mappedByteBuffer.force();
    }

    public long getPhysicMsgTimestamp() {
        return physicMsgTimestamp;
    }

    public void setPhysicMsgTimestamp(long physicMsgTimestamp) {
        this.physicMsgTimestamp = physicMsgTimestamp;
    }

    public long getLogicsMsgTimestamp() {
        return logicsMsgTimestamp;
    }

    public void setLogicsMsgTimestamp(long logicsMsgTimestamp) {
        this.logicsMsgTimestamp = logicsMsgTimestamp;
    }

    public long getConfirmPhyOffset() {
        return confirmPhyOffset;
    }

    public void setConfirmPhyOffset(long confirmPhyOffset) {
        this.confirmPhyOffset = confirmPhyOffset;
    }

    public long getMinTimestampIndex() {
        return Math.min(this.getMinTimestamp(), this.indexMsgTimestamp);
    }

    public long getMinTimestamp() {
        long min = Math.min(this.physicMsgTimestamp, this.logicsMsgTimestamp);

        min -= 1000 * 3;
        if (min < 0) {
            min = 0;
        }

        return min;
    }

    public long getIndexMsgTimestamp() {
        return indexMsgTimestamp;
    }

    public void setIndexMsgTimestamp(long indexMsgTimestamp) {
        this.indexMsgTimestamp = indexMsgTimestamp;
    }

    public long getMasterFlushedOffset() {
        return masterFlushedOffset;
    }

    public void setMasterFlushedOffset(long masterFlushedOffset) {
        this.masterFlushedOffset = masterFlushedOffset;
    }
}
