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
package org.apache.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageQueueAssignment;
import org.apache.rocketmq.common.message.MessageRequestMode;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.filter.FilterAPI;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public abstract class RebalanceImpl {
    protected static final Logger log = LoggerFactory.getLogger(RebalanceImpl.class);
    /*消息队列 ——> 该队列的消费状态*/
    protected final ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = new ConcurrentHashMap<>(64);
    protected final ConcurrentMap<MessageQueue, PopProcessQueue> popProcessQueueTable = new ConcurrentHashMap<>(64);
    /*【topicSubscribeInfoTable】维护消费者订阅的主题(键) 与 该主题下消息队列(值) 的映射关系 */
    protected final ConcurrentMap<String/* topic */, Set<MessageQueue>> topicSubscribeInfoTable =
        new ConcurrentHashMap<>();
    /*主题名称 ——> 订阅关系 的映射*/
    protected final ConcurrentMap<String /* topic */, SubscriptionData> subscriptionInner =
        new ConcurrentHashMap<>();
    protected String consumerGroup;
    protected MessageModel messageModel;
    protected AllocateMessageQueueStrategy allocateMessageQueueStrategy;
    protected MQClientInstance mQClientFactory;
    private static final int TIMEOUT_CHECK_TIMES = 3;
    private static final int QUERY_ASSIGNMENT_TIMEOUT = 3000;

    private Map<String, String> topicBrokerRebalance = new ConcurrentHashMap<>();
    private Map<String, String> topicClientRebalance = new ConcurrentHashMap<>();

    public RebalanceImpl(String consumerGroup, MessageModel messageModel,
        AllocateMessageQueueStrategy allocateMessageQueueStrategy,
        MQClientInstance mQClientFactory) {
        this.consumerGroup = consumerGroup;
        this.messageModel = messageModel;
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        this.mQClientFactory = mQClientFactory;
    }

    public void unlock(final MessageQueue mq, final boolean oneway) {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(this.mQClientFactory.getBrokerNameFromMessageQueue(mq), MixAll.MASTER_ID, true);
        if (findBrokerResult != null) {
            UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
            requestBody.setConsumerGroup(this.consumerGroup);
            requestBody.setClientId(this.mQClientFactory.getClientId());
            requestBody.getMqSet().add(mq);

            try {
                this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);
                log.warn("unlock messageQueue. group:{}, clientId:{}, mq:{}",
                    this.consumerGroup,
                    this.mQClientFactory.getClientId(),
                    mq);
            } catch (Exception e) {
                log.error("unlockBatchMQ exception, " + mq, e);
            }
        }
    }

    public void unlockAll(final boolean oneway) {
        HashMap<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();

        for (final Map.Entry<String, Set<MessageQueue>> entry : brokerMqs.entrySet()) {
            final String brokerName = entry.getKey();
            final Set<MessageQueue> mqs = entry.getValue();

            if (mqs.isEmpty()) {
                continue;
            }

            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
            if (findBrokerResult != null) {
                UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
                requestBody.setConsumerGroup(this.consumerGroup);
                requestBody.setClientId(this.mQClientFactory.getClientId());
                requestBody.setMqSet(mqs);

                try {
                    this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);

                    for (MessageQueue mq : mqs) {
                        ProcessQueue processQueue = this.processQueueTable.get(mq);
                        if (processQueue != null) {
                            processQueue.setLocked(false);
                            log.info("the message queue unlock OK, Group: {} {}", this.consumerGroup, mq);
                        }
                    }
                } catch (Exception e) {
                    log.error("unlockBatchMQ exception, " + mqs, e);
                }
            }
        }
    }

    /**【】根据processQueueTable构造出<String, Set<MessageQueue>>，方便下一步锁定Broker发送锁定消息队列请求*/
    private HashMap<String/* brokerName */, Set<MessageQueue>> buildProcessQueueTableByBrokerName() {
        HashMap<String /*BrokerName*/, Set<MessageQueue>> result = new HashMap<>();

        for (Map.Entry<MessageQueue, ProcessQueue> entry : this.processQueueTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            ProcessQueue pq = entry.getValue();
            //确保ProcessQueue是有效的
            if (pq.isDropped()) {
                continue;
            }
            //根据messageQueue获取brokerName
            String destBrokerName = this.mQClientFactory.getBrokerNameFromMessageQueue(mq);
            Set<MessageQueue> mqs = result.get(destBrokerName);
            if (null == mqs) {
                mqs = new HashSet<>();
                result.put(mq.getBrokerName(), mqs);
            }

            mqs.add(mq);
        }

        return result;
    }

    public boolean lock(final MessageQueue mq) {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(this.mQClientFactory.getBrokerNameFromMessageQueue(mq), MixAll.MASTER_ID, true);
        if (findBrokerResult != null) {
            LockBatchRequestBody requestBody = new LockBatchRequestBody();
            requestBody.setConsumerGroup(this.consumerGroup);
            requestBody.setClientId(this.mQClientFactory.getClientId());
            requestBody.getMqSet().add(mq);

            try {
                Set<MessageQueue> lockedMq =
                    this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);
                for (MessageQueue mmqq : lockedMq) {
                    ProcessQueue processQueue = this.processQueueTable.get(mmqq);
                    if (processQueue != null) {
                        processQueue.setLocked(true);
                        processQueue.setLastLockTimestamp(System.currentTimeMillis());
                    }
                }

                boolean lockOK = lockedMq.contains(mq);
                log.info("message queue lock {}, {} {}", lockOK ? "OK" : "Failed", this.consumerGroup, mq);
                return lockOK;
            } catch (Exception e) {
                log.error("lockBatchMQ exception, " + mq, e);
            }
        }

        return false;
    }

    public void lockAll() {
        HashMap<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();
        /*rocketmq中对于map的遍历，通常使用Iterator*/
        Iterator<Entry<String, Set<MessageQueue>>> it = brokerMqs.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Set<MessageQueue>> entry = it.next();
            final String brokerName = entry.getKey();
            final Set<MessageQueue> mqs = entry.getValue();

            if (mqs.isEmpty()) {
                continue;
            }

            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
            if (findBrokerResult != null) {
                LockBatchRequestBody requestBody = new LockBatchRequestBody();
                requestBody.setConsumerGroup(this.consumerGroup);
                requestBody.setClientId(this.mQClientFactory.getClientId());
                requestBody.setMqSet(mqs);

                try {
                    /*向Broker（主节点）发送锁定消息队列，该方法会返回成功被当前消费者锁定的消息消费队列*/
                    Set<MessageQueue> lockOKMQSet =
                        this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);

                    /*遍历所有的消息队列：
                        情况1：将成功锁定的消息队列 对应的 处理队列processQueue设置为锁定状态，并更新加锁时间戳；
                        情况2：如果当前消费者不持该消息队列的锁，则将处理队列锁的状态设置为false，暂停该消息消
                                费队列的消息拉取与消息消费。*/
                    for (MessageQueue mq : mqs) {
                        ProcessQueue processQueue = this.processQueueTable.get(mq);
                        if (processQueue != null) {
                            if (lockOKMQSet.contains(mq)) { //lockOKMQSet包含的话说明：这个消费者锁定了这个消息队列
                                if (!processQueue.isLocked()) {
                                    log.info("the message queue locked OK, Group: {} {}", this.consumerGroup, mq);
                                }
                                processQueue.setLocked(true);
                                processQueue.setLastLockTimestamp(System.currentTimeMillis());
                            } else { //lockOKMQSet不包含的话说明：这个消费者不持该消息队列的锁。因此不能接近processQueue
                                processQueue.setLocked(false);
                                log.warn("the message queue locked Failed, Group: {} {}", this.consumerGroup, mq);
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error("lockBatchMQ exception, " + mqs, e);
                }
            }
        }
    }

    public boolean clientRebalance(String topic) {
        return true;
    }

    /**【作用】是对所有的消息队列进行负载均衡。。。返回值表示是不是所有的消息队列都已经再平衡完成*/
    public boolean doRebalance(final boolean isOrder) {
        boolean balanced = true;
        /*拿到所有的订阅信息。
        * 【注意】每一个消费者内部持有一个属于自己的RebalanceImpl对象，因此这里拿到的订阅信息其实是当前消费者所有的订阅信息。
        *       在这个维度上，继续细化，这个订阅信息的键值是topic*/
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                final String topic = entry.getKey();
                /*依次从subscriptionInner拿出每一个topic，执行具体的再平衡逻辑，但是分两种情况：
                *   情况1：从Broker获取再平衡的结果
                *   情况2：本地计算再平衡*/
                try {
                    if (!clientRebalance(topic) && tryQueryAssignment(topic)) {
                        boolean result = this.getRebalanceResultFromBroker(topic, isOrder);
                        if (!result) {
                            balanced = false;
                        }
                    } else {
                        boolean result = this.rebalanceByTopic(topic, isOrder);
                        if (!result) {
                            balanced = false;
                        }
                    }
                } catch (Throwable e) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        log.warn("rebalance Exception", e);
                        balanced = false;
                    }
                }
            }
        }

        this.truncateMessageQueueNotMyTopic();

        return balanced;
    }

    /**判断当前消费者是否可以从 Broker 获取负载均衡的分配结果，而不是由客户端自行计算分配。*/
    private boolean tryQueryAssignment(String topic) {
        /*检查客户端 以及 broker端 是否已经进行过再平衡，如果已经进行过则直接返回*/
        if (topicClientRebalance.containsKey(topic)) {
            return false;
        }

        if (topicBrokerRebalance.containsKey(topic)) {
            return true;
        }
        String strategyName = allocateMessageQueueStrategy != null ? allocateMessageQueueStrategy.getName() : null;
        int retryTimes = 0;
        while (retryTimes++ < TIMEOUT_CHECK_TIMES) {
            try {
                Set<MessageQueueAssignment> resultSet = mQClientFactory.queryAssignment(topic, consumerGroup,
                    strategyName, messageModel, QUERY_ASSIGNMENT_TIMEOUT / TIMEOUT_CHECK_TIMES * retryTimes);
                topicBrokerRebalance.put(topic, topic);
                return true;
            } catch (Throwable t) {
                if (!(t instanceof RemotingTimeoutException)) {
                    log.error("tryQueryAssignment error.", t);
                    topicClientRebalance.put(topic, topic);
                    return false;
                }
            }
        }
        if (retryTimes >= TIMEOUT_CHECK_TIMES) {
            // if never success before and timeout exceed TIMEOUT_CHECK_TIMES, force client rebalance
            topicClientRebalance.put(topic, topic);
            return false;
        }
        return true;
    }

    public ConcurrentMap<String, SubscriptionData> getSubscriptionInner() {
        return subscriptionInner;
    }

    /**【作用】根据消费的类型(集群消费？广播消费？)，对指定主题进行负载均衡*/
    private boolean rebalanceByTopic(final String topic, final boolean isOrder) {
        boolean balanced = true;
        switch (messageModel) {
            case BROADCASTING: { //广播模式，每一个消费者都会消费主题的所有消息
                Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                /*如果mqSet不是null，则调用updateProcessQueueTableInRebalance进行更新；
                * 如果mqSet是null，则直接调用messageQueueChanged，实参传空集合即可*/
                if (mqSet != null) {
                    boolean changed = this.updateProcessQueueTableInRebalance(topic, mqSet, isOrder);
                    if (changed) {
                        this.messageQueueChanged(topic, mqSet, mqSet);
                        log.info("messageQueueChanged {} {} {} {}", consumerGroup, topic, mqSet, mqSet);
                    }

                    balanced = mqSet.equals(getWorkingMessageQueue(topic));
                } else {
                    this.messageQueueChanged(topic, Collections.<MessageQueue>emptySet(), Collections.<MessageQueue>emptySet());
                    log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                }
                break;
            }
            case CLUSTERING: {
                //1.从topicSubscribeInfoTable列表中获取与该topic相关的所有消息队列
                Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                //2. 从broker端获取消费该消费组的所有客户端clientId(思考：所有客户端id添加的时间)
                List<String> cidAll = this.mQClientFactory.findConsumerIdList(topic, consumerGroup);
                if (null == mqSet) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        this.messageQueueChanged(topic, Collections.<MessageQueue>emptySet(), Collections.<MessageQueue>emptySet());
                        log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                    }
                }

                if (null == cidAll) {
                    log.warn("doRebalance, {} {}, get consumer id list failed", consumerGroup, topic);
                }

                if (mqSet != null && cidAll != null) {
                    List<MessageQueue> mqAll = new ArrayList<>();
                    mqAll.addAll(mqSet);

                    Collections.sort(mqAll);
                    Collections.sort(cidAll);
                    // 获取分配策略，在创建DefaultMQPushConsumer对象时默认设置为AllocateMessageQueueAveragely，即平均分配
                    AllocateMessageQueueStrategy strategy = this.allocateMessageQueueStrategy;

                    List<MessageQueue> allocateResult = null;
                    try {
                        /*获取当前这个消费者对应分配到的队列集合allocateResult*/
                        allocateResult = strategy.allocate(
                            this.consumerGroup, /*消费者组名*/
                            this.mQClientFactory.getClientId(), /*当前的clientId，即消费者之前等级的*/
                            mqAll, /*所有的消息队列*/
                            cidAll /*所有的消费者*/);
                    } catch (Throwable e) {
                        log.error("allocate message queue exception. strategy name: {}, ex: {}", strategy.getName(), e);
                        return false;
                    }
                    /**【疑问】为什么上面得到的结果allocateResult不使用，而是下面重新声明一个set类型的
                     * allocateResultSet使用？？*/
                    Set<MessageQueue> allocateResultSet = new HashSet<>();
                    if (allocateResult != null) {
                        allocateResultSet.addAll(allocateResult);
                    }

                    boolean changed = this.updateProcessQueueTableInRebalance(topic, allocateResultSet, isOrder);
                    if (changed) {
                        log.info(
                            "client rebalanced result changed. allocateMessageQueueStrategyName={}, group={}, topic={}, clientId={}, mqAllSize={}, cidAllSize={}, rebalanceResultSize={}, rebalanceResultSet={}",
                            strategy.getName(), consumerGroup, topic, this.mQClientFactory.getClientId(), mqSet.size(), cidAll.size(),
                            allocateResultSet.size(), allocateResultSet);
                        this.messageQueueChanged(topic, mqSet, allocateResultSet);
                    }

                    balanced = allocateResultSet.equals(getWorkingMessageQueue(topic));
                }
                break;
            }
            default:
                break;
        }

        return balanced;
    }

    private boolean getRebalanceResultFromBroker(final String topic, final boolean isOrder) {
        String strategyName = this.allocateMessageQueueStrategy.getName();
        Set<MessageQueueAssignment> messageQueueAssignments;
        try {
            messageQueueAssignments = this.mQClientFactory.queryAssignment(topic, consumerGroup,
                strategyName, messageModel, QUERY_ASSIGNMENT_TIMEOUT);
        } catch (Exception e) {
            log.error("allocate message queue exception. strategy name: {}, ex: {}", strategyName, e);
            return false;
        }

        // null means invalid result, we should skip the update logic
        if (messageQueueAssignments == null) {
            return false;
        }
        Set<MessageQueue> mqSet = new HashSet<>();
        for (MessageQueueAssignment messageQueueAssignment : messageQueueAssignments) {
            if (messageQueueAssignment.getMessageQueue() != null) {
                mqSet.add(messageQueueAssignment.getMessageQueue());
            }
        }
        Set<MessageQueue> mqAll = null;
        boolean changed = this.updateMessageQueueAssignment(topic, messageQueueAssignments, isOrder);
        if (changed) {
            log.info("broker rebalanced result changed. allocateMessageQueueStrategyName={}, group={}, topic={}, clientId={}, assignmentSet={}",
                strategyName, consumerGroup, topic, this.mQClientFactory.getClientId(), messageQueueAssignments);
            this.messageQueueChanged(topic, mqAll, mqSet);
        }

        return mqSet.equals(getWorkingMessageQueue(topic));
    }

    private Set<MessageQueue> getWorkingMessageQueue(String topic) {
        Set<MessageQueue> queueSet = new HashSet<>();
        for (Entry<MessageQueue, ProcessQueue> entry : this.processQueueTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            ProcessQueue pq = entry.getValue();

            if (mq.getTopic().equals(topic) && !pq.isDropped()) {
                queueSet.add(mq);
            }
        }

        for (Entry<MessageQueue, PopProcessQueue> entry : this.popProcessQueueTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            PopProcessQueue pq = entry.getValue();

            if (mq.getTopic().equals(topic) && !pq.isDropped()) {
                queueSet.add(mq);
            }
        }

        return queueSet;
    }

    private void truncateMessageQueueNotMyTopic() {
        /*step1：获取当前消费者的订阅信息*/
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        /*step2:
        * 遍历processQueueTable、popProcessQueueTable,移除不属于当前订阅主题的消息队列*/
        for (MessageQueue mq : this.processQueueTable.keySet()) {
            if (!subTable.containsKey(mq.getTopic())) {

                ProcessQueue pq = this.processQueueTable.remove(mq);
                if (pq != null) {
                    pq.setDropped(true);
                    log.info("doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary mq, {}", consumerGroup, mq);
                }
            }
        }

        for (MessageQueue mq : this.popProcessQueueTable.keySet()) {
            if (!subTable.containsKey(mq.getTopic())) {

                PopProcessQueue pq = this.popProcessQueueTable.remove(mq);
                if (pq != null) {
                    pq.setDropped(true);
                    log.info("doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary pop mq, {}", consumerGroup, mq);
                }
            }
        }
        /*step3:
        清理topicClientRebalance和topicBrokerRebalance中不在属于当前订阅主题的条目*/
        Iterator<Map.Entry<String, String>> clientIter = topicClientRebalance.entrySet().iterator();
        while (clientIter.hasNext()) {
            if (!subTable.containsKey(clientIter.next().getKey())) {
                clientIter.remove();
            }
        }

        Iterator<Map.Entry<String, String>> brokerIter = topicBrokerRebalance.entrySet().iterator();
        while (brokerIter.hasNext()) {
            if (!subTable.containsKey(brokerIter.next().getKey())) {
                brokerIter.remove();
            }
        }
    }

    /**
     * @description:
     * @param topic:主题名称，表示当前需要重新平衡的消息队列所属的主题。
     * @param mqSet:一个集合，包含当前消费者应该处理的消息队列（MessageQueue）。
     * @param isOrder:布尔值，表示是否是顺序消费模式（Ordered Consumption）。如果是顺序消
     *                  费，需要额外的锁机制来保证消息的顺序性。
     * @return boolean: 表示是否发生了变化（即是否有消息队列被添加或移除）。
     * @author: Zhou
     * @date: 2025/3/12 0:33
     */
    private boolean updateProcessQueueTableInRebalance(final String topic, final Set<MessageQueue> mqSet,
        final boolean isOrder) {
        boolean changed = false;

        // drop process queues no longer belong me..移除不属于当前消费者处理的消息队列
        HashMap<MessageQueue, ProcessQueue> removeQueueMap = new HashMap<>(this.processQueueTable.size());
        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
        //遍历当前消费者维护的消息队列表(processQueueTable)。根据mqSet集合中的元素，判断当前消费者是否需要处理该消息队列。
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            MessageQueue mq = next.getKey();
            ProcessQueue pq = next.getValue();
            /*如果某个消息队列 mq 不再属于当前消费者（即不在 mqSet 中），或者该队列的拉取操
            作已过期（isPullExpired()），则将其标记为“丢弃”（setDropped(true)），并记录
            到 removeQueueMap 中。*/
            if (mq.getTopic().equals(topic)) {
                if (!mqSet.contains(mq)) {
                    pq.setDropped(true);
                    removeQueueMap.put(mq, pq);
                } else if (pq.isPullExpired() && this.consumeType() == ConsumeType.CONSUME_PASSIVELY) {
                    pq.setDropped(true);
                    removeQueueMap.put(mq, pq);
                    log.error("[BUG]doRebalance, {}, try remove unnecessary mq, {}, because pull is pause, so try to fixed it",
                        consumerGroup, mq);
                }
            }
        }

        // remove message queues no longer belong me...遍历 removeQueueMap 中的所有队列，调
        // 用 removeUnnecessaryMessageQueue 方法尝试删除这些队列。
        for (Entry<MessageQueue, ProcessQueue> entry : removeQueueMap.entrySet()) {
            MessageQueue mq = entry.getKey();
            ProcessQueue pq = entry.getValue();

            if (this.removeUnnecessaryMessageQueue(mq, pq)) {
                this.processQueueTable.remove(mq);
                changed = true;
                log.info("doRebalance, {}, remove unnecessary mq, {}", consumerGroup, mq);
            }
        }

        // add new message queue
        boolean allMQLocked = true;
        List<PullRequest> pullRequestList = new ArrayList<>();
        //for循环：遍历 mqSet 中的所有消息队列，检查是否已经存在于 processQueueTable 中。
        for (MessageQueue mq : mqSet) {
            //如果 mq 不存在于 processQueueTable中，进入处理逻辑。。(for循环中就只有这个if块)
            if (!this.processQueueTable.containsKey(mq)) {
                /*对于顺序消费模式（isOrder），尝试对消息队列加锁（lock(mq)）。如果加
                锁失败，则记录警告日志，并跳过该队列。*/
                if (isOrder && !this.lock(mq)) {
                    log.warn("doRebalance, {}, add a new mq failed, {}, because lock failed", consumerGroup, mq);
                    allMQLocked = false;
                    continue;
                }

                this.removeDirtyOffset(mq);
                //创建一个新的 ProcessQueue，并设置其状态为“已锁定”（setLocked(true)）。
                ProcessQueue pq = createProcessQueue(topic);
                pq.setLocked(true);
                /*计算从哪个偏移量开始拉取消息（computePullFromWhere(mq)）。如果计算结果
                有效（nextOffset >= 0），则将该队列添加到 processQueueTable 中，并创建一
                个拉取请求（PullRequest）*/
                long nextOffset = this.computePullFromWhere(mq);
                if (nextOffset >= 0) {
                    ProcessQueue pre = this.processQueueTable.putIfAbsent(mq, pq);
                    if (pre != null) {
                        log.info("doRebalance, {}, mq already exists, {}", consumerGroup, mq);
                    } else {
                        log.info("doRebalance, {}, add a new mq, {}", consumerGroup, mq);
                        PullRequest pullRequest = new PullRequest();
                        pullRequest.setConsumerGroup(consumerGroup);
                        pullRequest.setNextOffset(nextOffset);
                        pullRequest.setMessageQueue(mq);
                        pullRequest.setProcessQueue(pq);
                        //把拉取请求加入 pullRequestList，并将 changed 标志设置为 true。
                        pullRequestList.add(pullRequest);
                        changed = true;
                    }
                } else {
                    log.warn("doRebalance, {}, add new mq failed, {}", consumerGroup, mq);
                }
            }

        }
        /*如果有消息队列未能成功加锁（allMQLocked == false），则延迟 500 毫秒后重新触
        发重新平衡（rebalanceLater）————仅仅针对顺序消息(顺序消息才需要枷锁)*/
        if (!allMQLocked) {
            mQClientFactory.rebalanceLater(500);
        }
        //将所有新创建的拉取请求（pullRequestList）分发给消费者进行处理，延迟时间为 500 毫秒。
        this.dispatchPullRequest(pullRequestList, 500);

        return changed;
    }

    private boolean updateMessageQueueAssignment(final String topic, final Set<MessageQueueAssignment> assignments,
        final boolean isOrder) {
        boolean changed = false;

        Map<MessageQueue, MessageQueueAssignment> mq2PushAssignment = new HashMap<>();
        Map<MessageQueue, MessageQueueAssignment> mq2PopAssignment = new HashMap<>();
        for (MessageQueueAssignment assignment : assignments) {
            MessageQueue messageQueue = assignment.getMessageQueue();
            if (messageQueue == null) {
                continue;
            }
            if (MessageRequestMode.POP == assignment.getMode()) {
                mq2PopAssignment.put(messageQueue, assignment);
            } else {
                mq2PushAssignment.put(messageQueue, assignment);
            }
        }

        if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
            if (mq2PopAssignment.isEmpty() && !mq2PushAssignment.isEmpty()) {
                //pop switch to push
                //subscribe pop retry topic
                try {
                    final String retryTopic = KeyBuilder.buildPopRetryTopic(topic, getConsumerGroup());
                    SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(retryTopic, SubscriptionData.SUB_ALL);
                    getSubscriptionInner().put(retryTopic, subscriptionData);
                } catch (Exception ignored) {
                }

            } else if (!mq2PopAssignment.isEmpty() && mq2PushAssignment.isEmpty()) {
                //push switch to pop
                //unsubscribe pop retry topic
                try {
                    final String retryTopic = KeyBuilder.buildPopRetryTopic(topic, getConsumerGroup());
                    getSubscriptionInner().remove(retryTopic);
                } catch (Exception ignored) {
                }

            }
        }

        {
            // drop process queues no longer belong me
            HashMap<MessageQueue, ProcessQueue> removeQueueMap = new HashMap<>(this.processQueueTable.size());
            Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<MessageQueue, ProcessQueue> next = it.next();
                MessageQueue mq = next.getKey();
                ProcessQueue pq = next.getValue();

                if (mq.getTopic().equals(topic)) {
                    if (!mq2PushAssignment.containsKey(mq)) {
                        pq.setDropped(true);
                        removeQueueMap.put(mq, pq);
                    } else if (pq.isPullExpired() && this.consumeType() == ConsumeType.CONSUME_PASSIVELY) {
                        pq.setDropped(true);
                        removeQueueMap.put(mq, pq);
                        log.error("[BUG]doRebalance, {}, try remove unnecessary mq, {}, because pull is pause, so try to fixed it",
                            consumerGroup, mq);
                    }
                }
            }
            // remove message queues no longer belong me
            for (Entry<MessageQueue, ProcessQueue> entry : removeQueueMap.entrySet()) {
                MessageQueue mq = entry.getKey();
                ProcessQueue pq = entry.getValue();

                if (this.removeUnnecessaryMessageQueue(mq, pq)) {
                    this.processQueueTable.remove(mq);
                    changed = true;
                    log.info("doRebalance, {}, remove unnecessary mq, {}", consumerGroup, mq);
                }
            }
        }

        {
            HashMap<MessageQueue, PopProcessQueue> removeQueueMap = new HashMap<>(this.popProcessQueueTable.size());
            Iterator<Entry<MessageQueue, PopProcessQueue>> it = this.popProcessQueueTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<MessageQueue, PopProcessQueue> next = it.next();
                MessageQueue mq = next.getKey();
                PopProcessQueue pq = next.getValue();

                if (mq.getTopic().equals(topic)) {
                    if (!mq2PopAssignment.containsKey(mq)) {
                        //the queue is no longer your assignment
                        pq.setDropped(true);
                        removeQueueMap.put(mq, pq);
                    } else if (pq.isPullExpired() && this.consumeType() == ConsumeType.CONSUME_PASSIVELY) {
                        pq.setDropped(true);
                        removeQueueMap.put(mq, pq);
                        log.error("[BUG]doRebalance, {}, try remove unnecessary pop mq, {}, because pop is pause, so try to fixed it",
                            consumerGroup, mq);
                    }
                }
            }
            // remove message queues no longer belong me
            for (Entry<MessageQueue, PopProcessQueue> entry : removeQueueMap.entrySet()) {
                MessageQueue mq = entry.getKey();
                PopProcessQueue pq = entry.getValue();

                if (this.removeUnnecessaryPopMessageQueue(mq, pq)) {
                    this.popProcessQueueTable.remove(mq);
                    changed = true;
                    log.info("doRebalance, {}, remove unnecessary pop mq, {}", consumerGroup, mq);
                }
            }
        }

        {
            // add new message queue
            boolean allMQLocked = true;
            List<PullRequest> pullRequestList = new ArrayList<>();
            for (MessageQueue mq : mq2PushAssignment.keySet()) {
                if (!this.processQueueTable.containsKey(mq)) {
                    if (isOrder && !this.lock(mq)) {
                        log.warn("doRebalance, {}, add a new mq failed, {}, because lock failed", consumerGroup, mq);
                        allMQLocked = false;
                        continue;
                    }

                    this.removeDirtyOffset(mq);
                    ProcessQueue pq = createProcessQueue();
                    pq.setLocked(true);
                    long nextOffset = -1L;
                    try {
                        nextOffset = this.computePullFromWhereWithException(mq);
                    } catch (Exception e) {
                        log.info("doRebalance, {}, compute offset failed, {}", consumerGroup, mq);
                        continue;
                    }

                    if (nextOffset >= 0) {
                        ProcessQueue pre = this.processQueueTable.putIfAbsent(mq, pq);
                        if (pre != null) {
                            log.info("doRebalance, {}, mq already exists, {}", consumerGroup, mq);
                        } else {
                            log.info("doRebalance, {}, add a new mq, {}", consumerGroup, mq);
                            PullRequest pullRequest = new PullRequest();
                            pullRequest.setConsumerGroup(consumerGroup);
                            pullRequest.setNextOffset(nextOffset);
                            pullRequest.setMessageQueue(mq);
                            pullRequest.setProcessQueue(pq);
                            pullRequestList.add(pullRequest);
                            changed = true;
                        }
                    } else {
                        log.warn("doRebalance, {}, add new mq failed, {}", consumerGroup, mq);
                    }
                }
            }

            if (!allMQLocked) {
                mQClientFactory.rebalanceLater(500);
            }
            this.dispatchPullRequest(pullRequestList, 500);
        }

        {
            // add new message queue
            List<PopRequest> popRequestList = new ArrayList<>();
            for (MessageQueue mq : mq2PopAssignment.keySet()) {
                if (!this.popProcessQueueTable.containsKey(mq)) {
                    PopProcessQueue pq = createPopProcessQueue();
                    PopProcessQueue pre = this.popProcessQueueTable.putIfAbsent(mq, pq);
                    if (pre != null) {
                        log.info("doRebalance, {}, mq pop already exists, {}", consumerGroup, mq);
                    } else {
                        log.info("doRebalance, {}, add a new pop mq, {}", consumerGroup, mq);
                        PopRequest popRequest = new PopRequest();
                        popRequest.setTopic(topic);
                        popRequest.setConsumerGroup(consumerGroup);
                        popRequest.setMessageQueue(mq);
                        popRequest.setPopProcessQueue(pq);
                        popRequest.setInitMode(getConsumeInitMode());
                        popRequestList.add(popRequest);
                        changed = true;
                    }
                }
            }

            this.dispatchPopPullRequest(popRequestList, 500);
        }

        return changed;
    }

    public abstract void messageQueueChanged(final String topic, final Set<MessageQueue> mqAll,
        final Set<MessageQueue> mqDivided);

    public abstract boolean removeUnnecessaryMessageQueue(final MessageQueue mq, final ProcessQueue pq);

    public boolean removeUnnecessaryPopMessageQueue(final MessageQueue mq, final PopProcessQueue pq) {
        return true;
    }

    public abstract ConsumeType consumeType();

    public abstract void removeDirtyOffset(final MessageQueue mq);

    /**
     * When the network is unstable, using this interface may return wrong offset.
     * It is recommended to use computePullFromWhereWithException instead.
     * @param mq
     * @return offset
     */
    @Deprecated
    public abstract long computePullFromWhere(final MessageQueue mq);

    public abstract long computePullFromWhereWithException(final MessageQueue mq) throws MQClientException;

    public abstract int getConsumeInitMode();

    public abstract void dispatchPullRequest(final List<PullRequest> pullRequestList, final long delay);

    public abstract void dispatchPopPullRequest(final List<PopRequest> pullRequestList, final long delay);

    public abstract ProcessQueue createProcessQueue();

    public abstract PopProcessQueue createPopProcessQueue();

    public abstract ProcessQueue createProcessQueue(String topicName);

    public void removeProcessQueue(final MessageQueue mq) {
        ProcessQueue prev = this.processQueueTable.remove(mq);
        if (prev != null) {
            boolean droped = prev.isDropped();
            prev.setDropped(true);
            this.removeUnnecessaryMessageQueue(mq, prev);
            log.info("Fix Offset, {}, remove unnecessary mq, {} Droped: {}", consumerGroup, mq, droped);
        }
    }

    public ConcurrentMap<MessageQueue, ProcessQueue> getProcessQueueTable() {
        return processQueueTable;
    }

    public ConcurrentMap<MessageQueue, PopProcessQueue> getPopProcessQueueTable() {
        return popProcessQueueTable;
    }

    public ConcurrentMap<String, Set<MessageQueue>> getTopicSubscribeInfoTable() {
        return topicSubscribeInfoTable;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
        return allocateMessageQueueStrategy;
    }

    public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }

    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }

    public void setmQClientFactory(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    public void destroy() {
        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            next.getValue().setDropped(true);
        }

        this.processQueueTable.clear();

        Iterator<Entry<MessageQueue, PopProcessQueue>> popIt = this.popProcessQueueTable.entrySet().iterator();
        while (popIt.hasNext()) {
            Entry<MessageQueue, PopProcessQueue> next = popIt.next();
            next.getValue().setDropped(true);
        }
        this.popProcessQueueTable.clear();
    }

}
