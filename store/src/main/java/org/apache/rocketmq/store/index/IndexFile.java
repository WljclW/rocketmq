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
package org.apache.rocketmq.store.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.List;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.logfile.DefaultMappedFile;
import org.apache.rocketmq.store.logfile.MappedFile;

/**
 * 【】默认一个Index文件包含2000万个条目
 * rocketmq除了按照主题订阅消息，还引入了哈希索引机制
 * RocketMQ引入哈希索引机制为消息建立索引，HashMap的设计包含两个基本点：哈希槽
 *      与哈希冲突的链表结构
 * */
public class IndexFile {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static int hashSlotSize = 4; //每一个哈希槽占4字节
    /**
     * Each index's store unit. Format:
     * <pre>
     * ┌───────────────┬───────────────────────────────┬───────────────┬───────────────┐
     * │ Key HashCode  │        Physical Offset        │   Time Diff   │ Next Index Pos│
     * │   (4 Bytes)   │          (8 Bytes)            │   (4 Bytes)   │   (4 Bytes)   │
     * ├───────────────┴───────────────────────────────┴───────────────┴───────────────┤
     * │                                 Index Store Unit                              │
     * │                                                                               │
     * </pre>
     * Each index's store unit. Size:
     * Key HashCode(4) + Physical Offset(8) + Time Diff(4) + Next Index Pos(4) = 20 Bytes
     */
    private static int indexSize = 20;
    private static int invalidIndex = 0;
    private final int hashSlotNum; //哈希槽的数量
    private final int indexNum;
    private final int fileTotalSize;
    private final MappedFile mappedFile;
    private final MappedByteBuffer mappedByteBuffer;
    private final IndexHeader indexHeader;

    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum,
        final long endPhyOffset, final long endTimestamp) throws IOException {
        this.fileTotalSize =
            IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
        this.mappedFile = new DefaultMappedFile(fileName, fileTotalSize);
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        this.hashSlotNum = hashSlotNum;
        this.indexNum = indexNum;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        this.indexHeader = new IndexHeader(byteBuffer);

        if (endPhyOffset > 0) {
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }

        if (endTimestamp > 0) {
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    public int getFileSize() {
        return this.fileTotalSize;
    }

    public void load() {
        this.indexHeader.load();
    }

    public void shutdown() {
        this.flush();
        UtilAll.cleanBuffer(this.mappedByteBuffer);
    }

    public void flush() {
        long beginTime = System.currentTimeMillis();
        if (this.mappedFile.hold()) {
            this.indexHeader.updateByteBuffer();
            this.mappedByteBuffer.force();
            this.mappedFile.release();
            log.info("flush index file elapsed time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    public boolean isWriteFull() {
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }

    /**
     * 【总述】根据key、消息在CommitLog的偏移量等，创建index条目放入到Index文件
     * 【方法的逻辑】
     *      先是根据key计算出哈希槽的索引(取余法)；记录拿到哈希槽的值(如果值无效的话就记录为0)；记录时间差值；计算index条目应该放值的偏移量；
     *      然后依次将index条目的各个部分放进去(step2记录的哈希槽的值放在最后4字节)
     * 【头插法】step1:由于每一次是先拿出对应哈希槽的值(可以叫做 旧值)，step2:然后将这个"旧值"写入当前的index条目的末尾4字节中，
     *      step3:最后更将哈希槽中的旧值更新为当前index条目的索引
     *      ————从上述逻辑可以看出来，最后插入的index条目是
     * 【其他想法】
     * key：消息索引；phyOffset：消息物理偏移量；storeTimestamp：消息存储时间戳
     * 这个方法有一个严密的逻辑：
     *      如果哈希槽中存储的值为0或大于当前Index文件最大条目数或小于-1，表示该哈希槽当前并没有与之对应的Index条目。(这样做相当于把哈希
     *          冲突的问题就解决了)
     *      如果存储的值大于0，表示该哈希槽当前有与之对应的Index条目，此时需要去获取该Index条目的物理地址，然后通过物理地址获取到该Index
     *          条目，(然后每一个index条目的最后4字节会存放哈希冲突链中前一个index条目)
     * */
    //将 消息索引键 与 消息偏移量 的映射关系写入index文件的实现。。重点关注一下方法中的绿色注释
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        if (this.indexHeader.getIndexCount() < this.indexNum) {
            int keyHash = indexKeyHashMethod(key); //计算key的哈希码
            int slotPos = keyHash % this.hashSlotNum; //计算key对应的哈希槽的下标
            //计算key对应的哈希槽 的物理地址(40字节的头部+下标*4字节index文件每个条目的长度)
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            try {
                /**
                 * 这里会拿到哈希槽对应的值
                 * */
                //这里会去拿参数key对应的哈希槽(每一个都是4字节)现在的值。。
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos); //拿出存当前消息前，key对应哈希槽的值————这个值就是在最后的索引
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    //进入到if条件说明：当前哈希槽是没有东西的————也就是说目前还没有key映射到这个哈希槽。。
                    slotValue = invalidIndex; //将哈希槽的值置为0
                }

                //计算带存储消息 与 第一条消息时间戳 的差值，并换算为秒
                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();
                timeDiff = timeDiff / 1000;

                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }
                /*计算新添加条目的起始物理偏移量absIndexPos————
                       40字节的头部(固定40字节) +
                       哈希槽数量(通过this.hashSlotNum拿)*每一个哈希槽所占字节数(固定值4字节) +
                       当前文件中index条目数量(通过this.indexHeader.getIndexCount()拿)*每个条目所占的字节数(固定值20字节)
                 */
                int absIndexPos =
                    IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                        + this.indexHeader.getIndexCount() * indexSize;
                //下面依次将 哈希码、物理偏移量、时间差、
                this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                /**
                 * 这里会把上面拿到哈希槽的值赋值给index条目的最后4字节。(这样当某一个哈希槽有很多key被映射到的时候，所有的index条目就会
                 *      形成一个链条，每一个index条目的最后4字节 就是 这个链中前一个index条目对应的索引)
                 * */
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue); //index条目的最后4字节存储该哈希码上一个条目的index下标
                //哈希槽中存储的是该哈希码对应的最新index条目的下标
                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());
                //更新index文件头部的信息
                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }

                if (invalidIndex == slotValue) {
                    this.indexHeader.incHashSlotCount();
                }
                this.indexHeader.incIndexCount();
                this.indexHeader.setEndPhyOffset(phyOffset);
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            }
        } else {
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
                + "; index max num = " + this.indexNum);
        }

        return false;
    }

    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0) {
            keyHashPositive = 0;
        }
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result || begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp();
        result = result || end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp();
        return result;
    }

    /**
     * 参数解释：
     *      phyOffsets:要查找的消息的物理偏移量
     *      key:要查找的消息的key
     *      maxNum:最多查到多少条消息
     *      begin:查询的起始时间戳
     *      end:查询的结束时间戳
     * 【总述】用于根据指定的key(参数key) 和 时间戳范围(参数begin~end决定) 查找满足条件的至多maxnNum条消息，并将这些消息的物理偏移
     *      量放入参数phyOffsets中。
     * */
    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
                                final long begin, final long end) {
        if (this.mappedFile.hold()) {
            int keyHash = indexKeyHashMethod(key);  //计算出哈希
            int slotPos = keyHash % this.hashSlotNum; //根据哈希值取余哈希槽数量，得到索引
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize; //拿到该索引哈希槽的起始偏移

            try {
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                        || this.indexHeader.getIndexCount() <= 1) { //如果这个值无效(说明没有前置的index条目，即这个哈希槽只有这一个条目)，什么也不做
                } else {
                    for (int nextIndexToRead = slotValue; ; ) {
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }
                        //拿到映射到这个哈希槽的某个index条目的偏移
                        int absIndexPos =
                            IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                + nextIndexToRead * indexSize;

                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);

                        long timeDiff = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        if (timeDiff < 0) {
                            break;
                        }

                        timeDiff *= 1000L;

                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        boolean timeMatched = timeRead >= begin && timeRead <= end;

                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);  /**表示找到了一个 满足条件的消息，将它的偏移量放入到phyOffsets*/
                        }

                        if (prevIndexRead <= invalidIndex
                            || prevIndexRead > this.indexHeader.getIndexCount()
                            || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }

                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                this.mappedFile.release();
            }
        }
    }
}
