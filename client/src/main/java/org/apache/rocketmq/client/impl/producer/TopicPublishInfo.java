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
package org.apache.rocketmq.client.impl.producer;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.rocketmq.client.common.ThreadLocalIndex;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

public class TopicPublishInfo {
    private boolean orderTopic = false;     //是否是顺序消息
    private boolean haveTopicRouterInfo = false;
    private List<MessageQueue> messageQueueList = new ArrayList<>(); //该主题对应的所有消息队列
    private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();  //每选择一次消息队列， 该值会自增1，如果 Integer.MAX_VALUE, 则重置为0，用于选择消息队列
    private TopicRouteData topicRouteData;  //路由元数据

    public interface QueueFilter {
        boolean filter(MessageQueue mq);
    }

    public boolean isOrderTopic() {
        return orderTopic;
    }

    public void setOrderTopic(boolean orderTopic) {
        this.orderTopic = orderTopic;
    }

    public boolean ok() {
        return null != this.messageQueueList && !this.messageQueueList.isEmpty();
    }

    public List<MessageQueue> getMessageQueueList() {
        return messageQueueList;
    }

    public void setMessageQueueList(List<MessageQueue> messageQueueList) {
        this.messageQueueList = messageQueueList;
    }

    public ThreadLocalIndex getSendWhichQueue() {
        return sendWhichQueue;
    }

    public void setSendWhichQueue(ThreadLocalIndex sendWhichQueue) {
        this.sendWhichQueue = sendWhichQueue;
    }

    public boolean isHaveTopicRouterInfo() {
        return haveTopicRouterInfo;
    }

    public void setHaveTopicRouterInfo(boolean haveTopicRouterInfo) {
        this.haveTopicRouterInfo = haveTopicRouterInfo;
    }

    /**
     * 首先在一次消息发送过程中，可能会多次执行选择消息队列这个方法， lastBrokerName 就是上一次选择的执行发送消息失败的Broker。
     *      第一次执行消息队列选择时，lastBrokerName 为 null，此时直接用 sendWhichQueue 自增再获取值， 与当前路由表中消息队
     *      列个数取模， 返回该位置的消息队列，如果消息发送再失败的话， 下次进行消息队列选择时规避上次MesageQueue所在的Broker，
     *      否则还是很有可能再次失败。
     *
     * 该算法在一次消息发送过程中能成功规避故障的Broker，但如果Broker宕机，由于路由算法中的消息队列是按Broker排序的，如果上一次
     *      根据路由算法选择的是宕机的Broker的第一个队列，那么随后的下次选择的是宕机Broker的第二个队列，消息发送很有可能会失败，
     *      再次引发重试，带来不必要的性能损耗，那么有什么方法在一次消息发送失败后，暂时将该Broker排除在消息队列选择范围外呢？或
     *      许有朋友会问， Broker不可用后，路由信息中为什么还会包含该Brok町的路由信息呢？其实这不难解释：首先， NameServer检测
     *      Broker 是否可用是有延迟的，最短为一次心跳检测间隔（5s）； 其次， NameServer不会检测到Broker宕机后马上推送消息给消
     *      息生产者，而是消息生产者每隔30s更新一次路由信息，所以消息生产者最快感知Broker最新的路由信息也需要30s。 如果能引人一
     *      种机制，在Broker 宕机期间，如果一次消息发送失败后，可以将该Broker暂时排除在消息队列的选择范围中。
     *
     * 另外：BrokerFilter 可以理解为名称过滤器，内部存放了上次异常的 Broker 名称。然后在选择队列的时候，排除掉这个 Broker。
     * */
    public MessageQueue selectOneMessageQueue(QueueFilter ...filter) {
        return selectOneMessageQueue(this.messageQueueList, this.sendWhichQueue, filter);
    }

    private MessageQueue selectOneMessageQueue(List<MessageQueue> messageQueueList, ThreadLocalIndex sendQueue, QueueFilter ...filter) {
        if (messageQueueList == null || messageQueueList.isEmpty()) {
            return null;
        }
        /**
         * 根据过滤器筛选队列的逻辑如下：
         * 根据ThreadLocalIndex拿队列，拿到队列循环遍历过滤器。。。有两种结果
         *       ————如果能选到队列(即filterResult==true),则返回。
         *       ————如果选不到队列，则返回null
         * */
        //如果存在过滤器，使用下面的if块进行筛选
        if (filter != null && filter.length != 0) {
            for (int i = 0; i < messageQueueList.size(); i++) {
                int index = Math.abs(sendQueue.incrementAndGet() % messageQueueList.size());
                MessageQueue mq = messageQueueList.get(index);
                boolean filterResult = true;
                for (QueueFilter f: filter) {
                    Preconditions.checkNotNull(f);
                    filterResult &= f.filter(mq);
                }
                if (filterResult) {
                    return mq;
                }
            }

            return null;
        }
        //如果filter是null，就直接取模返回对应的消息队列
        int index = Math.abs(sendQueue.incrementAndGet() % messageQueueList.size());
        return messageQueueList.get(index);
    }

    public void resetIndex() {
        this.sendWhichQueue.reset();
    }

    //如果sendLatencyFaultEnable=false,调用下面方法
    /**
     * 在消息发送过程中，可能会多次执行选择消息队列这个方法，lastBrokerName就是上一次选择的执行发送消息失败的Broker。第一
     * 次执行消息队列选择时，lastBrokerName为null，此时直接用sendWhichQueue自增再获取值，与当前路由表中消息队列的个数取
     * 模，返回该位置的MessageQueue(selectOneMessageQueue()方法，如果消息发送失败，下次进行消息队列选择时规避上次MesageQueue所在
     * 的Broker，否则有可能再次失败.
     * */
    public MessageQueue selectOneMessageQueue(final String lastBrokerName) {
        if (lastBrokerName == null) {
            return selectOneMessageQueue();
        } else {
            for (int i = 0; i < this.messageQueueList.size(); i++) {
                int index = this.sendWhichQueue.incrementAndGet();
                int pos = index % this.messageQueueList.size();
                MessageQueue mq = this.messageQueueList.get(pos);
                //下面的逻辑会避免选择上一次的broker。原因：这个else语句块表示已经发生失败了，这次属于"消息发送重试"的情况
                if (!mq.getBrokerName().equals(lastBrokerName)) {
                    return mq;
                }
            }
            return selectOneMessageQueue();
        }
    }

    /**
     * 根据 MQFaultStrategy#selectOneMessageQueue(TopicPublishInfo, java.lang.String, boolean) 的逻辑可以知道：
     *      下面的方法是一个兜底的逻辑
     * */
    public MessageQueue selectOneMessageQueue() {
        int index = this.sendWhichQueue.incrementAndGet();
        int pos = index % this.messageQueueList.size();

        return this.messageQueueList.get(pos);
    }

    public int getWriteQueueNumsByBroker(final String brokerName) {
        for (int i = 0; i < topicRouteData.getQueueDatas().size(); i++) {
            final QueueData queueData = this.topicRouteData.getQueueDatas().get(i);
            if (queueData.getBrokerName().equals(brokerName)) {
                return queueData.getWriteQueueNums();
            }
        }

        return -1;
    }

    @Override
    public String toString() {
        return "TopicPublishInfo [orderTopic=" + orderTopic + ", messageQueueList=" + messageQueueList
            + ", sendWhichQueue=" + sendWhichQueue + ", haveTopicRouterInfo=" + haveTopicRouterInfo + "]";
    }

    public TopicRouteData getTopicRouteData() {
        return topicRouteData;
    }

    public void setTopicRouteData(final TopicRouteData topicRouteData) {
        this.topicRouteData = topicRouteData;
    }
}
