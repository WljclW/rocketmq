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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**用于表示从存储层（如 CommitLog 或 ConsumeQueue）拉取消息的结果的一个类。它的主要作用是封装从存储层读取到的消息数据及其
 * 相关元信息，供消费者或调用方使用。*/
public class GetMessageResult {
    /*存储所有从存储层读取到的消息缓冲区对象（SelectMappedBufferResult）；
    * 每个 SelectMappedBufferResult 对象封装了一段内存映射文件中的数据，表示一条或多条消息的内容。
    调用方可以通过这些对象访问消息的实际内容。*/
    private final List<SelectMappedBufferResult> messageMapedList;
    /*用于存储所有消息的字节缓冲区（ByteBuffer）。字节缓冲区包含了消息的实际内容。
    与messageMapedList的区别：messageMapedList仅仅关注消息的实际内容
    */
    private final List<ByteBuffer> messageBufferList;
    /*这是一个列表，用于存储（这个结果集中）每条消息的逻辑偏移量（queueOffset）。
        逻辑偏移量用于标识消息在消费队列中的位置。*/
    private final List<Long> messageQueueOffset;
    /*标识消息拉取的状态*/
    private GetMessageStatus status;
    private long nextBeginOffset;
    //当前结果集中包含的消息的最小偏移量。
    private long minOffset;
    //当前结果集中包含的消息的最大偏移量。
    private long maxOffset;
    //当前结果集中所有消息的总字节数。
    private int bufferTotalSize = 0;
    //当前结果集中包含的消息总数。
    private int messageCount = 0;
    //是否建议 从从节点拉取消息
    private boolean suggestPullingFromSlave = false;

    private int msgCount4Commercial = 0;
    private int commercialSizePerMsg = 4 * 1024;
    //冷数据总量
    private long coldDataSum = 0L;

    public static final GetMessageResult NO_MATCH_LOGIC_QUEUE =
        new GetMessageResult(GetMessageStatus.NO_MATCHED_LOGIC_QUEUE, 0, 0, 0, Collections.emptyList(),
            Collections.emptyList(), Collections.emptyList());

    public GetMessageResult() {
        messageMapedList = new ArrayList<>(100);
        messageBufferList = new ArrayList<>(100);
        messageQueueOffset = new ArrayList<>(100);
    }

    public GetMessageResult(int resultSize) {
        messageMapedList = new ArrayList<>(resultSize);
        messageBufferList = new ArrayList<>(resultSize);
        messageQueueOffset = new ArrayList<>(resultSize);
    }

    private GetMessageResult(GetMessageStatus status, long nextBeginOffset, long minOffset, long maxOffset,
        List<SelectMappedBufferResult> messageMapedList, List<ByteBuffer> messageBufferList, List<Long> messageQueueOffset) {
        this.status = status;
        this.nextBeginOffset = nextBeginOffset;
        this.minOffset = minOffset;
        this.maxOffset = maxOffset;
        this.messageMapedList = messageMapedList;
        this.messageBufferList = messageBufferList;
        this.messageQueueOffset = messageQueueOffset;
    }

    public GetMessageStatus getStatus() {
        return status;
    }

    public void setStatus(GetMessageStatus status) {
        this.status = status;
    }

    public long getNextBeginOffset() {
        return nextBeginOffset;
    }

    public void setNextBeginOffset(long nextBeginOffset) {
        this.nextBeginOffset = nextBeginOffset;
    }

    public long getMinOffset() {
        return minOffset;
    }

    public void setMinOffset(long minOffset) {
        this.minOffset = minOffset;
    }

    public long getMaxOffset() {
        return maxOffset;
    }

    public void setMaxOffset(long maxOffset) {
        this.maxOffset = maxOffset;
    }

    public List<SelectMappedBufferResult> getMessageMapedList() {
        return messageMapedList;
    }

    public List<ByteBuffer> getMessageBufferList() {
        return messageBufferList;
    }

    public void addMessage(final SelectMappedBufferResult mapedBuffer) {
        this.messageMapedList.add(mapedBuffer);
        this.messageBufferList.add(mapedBuffer.getByteBuffer());
        this.bufferTotalSize += mapedBuffer.getSize();
        this.msgCount4Commercial += (int) Math.ceil(
            mapedBuffer.getSize() /  (double)commercialSizePerMsg);
        this.messageCount++;
    }

    /**将从存储中读取的消息（SelectMappedBufferResult）添加到结果集中。它的
     * 主要功能是更新消息的元信息、统计数据以及相关的计数器*/
    public void addMessage(final SelectMappedBufferResult mapedBuffer, final long queueOffset) {
        this.messageMapedList.add(mapedBuffer);
        this.messageBufferList.add(mapedBuffer.getByteBuffer());
        this.bufferTotalSize += mapedBuffer.getSize();
        this.msgCount4Commercial += (int) Math.ceil(
            mapedBuffer.getSize() /  (double)commercialSizePerMsg);
        this.messageCount++;
        this.messageQueueOffset.add(queueOffset);
    }


    /**用于将从存储中读取的消息添加到 GetMessageResult 对象中。这个方法在消息拉取过程中被
     * 调用，用于逐步构建返回给消费者的消息结果集。
     * @param mapedBuffer 存表示从 commitLog 中读取到的消息内容的封装对象（内存缓冲区的封装）
     * @param queueOffset 消息在消费队列中的逻辑偏移量（queueOffset）
     * @param batchNum 当前消息批次的数量，表示该消息单元中包含的消息条数（通常是 1，但在批量消息场景下可能大于 1）。*/
    public void addMessage(final SelectMappedBufferResult mapedBuffer, final long queueOffset, final int batchNum) {
        addMessage(mapedBuffer, queueOffset);
        messageCount += batchNum - 1;
    }

    public void release() {
        for (SelectMappedBufferResult select : this.messageMapedList) {
            select.release();
        }
    }

    public int getBufferTotalSize() {
        return bufferTotalSize;
    }

    public int getMessageCount() {
        return messageCount;
    }

    public boolean isSuggestPullingFromSlave() {
        return suggestPullingFromSlave;
    }

    public void setSuggestPullingFromSlave(boolean suggestPullingFromSlave) {
        this.suggestPullingFromSlave = suggestPullingFromSlave;
    }

    public int getMsgCount4Commercial() {
        return msgCount4Commercial;
    }

    public void setMsgCount4Commercial(int msgCount4Commercial) {
        this.msgCount4Commercial = msgCount4Commercial;
    }

    public List<Long> getMessageQueueOffset() {
        return messageQueueOffset;
    }

    public long getColdDataSum() {
        return coldDataSum;
    }

    public void setColdDataSum(long coldDataSum) {
        this.coldDataSum = coldDataSum;
    }

    @Override
    public String toString() {
        return "GetMessageResult [status=" + status + ", nextBeginOffset=" + nextBeginOffset + ", minOffset="
            + minOffset + ", maxOffset=" + maxOffset + ", bufferTotalSize=" + bufferTotalSize + ", messageCount=" + messageCount
            + ", suggestPullingFromSlave=" + suggestPullingFromSlave + "]";
    }
}
