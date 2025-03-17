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
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.consumer.AckCallback;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageQueueListener;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.PopCallback;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.consumer.store.LocalFileOffsetStore;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.consumer.store.RemoteBrokerOffsetStore;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.client.hook.FilterMessageContext;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.ConsumeStatus;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.body.PopProcessQueueInfo;
import org.apache.rocketmq.remoting.protocol.body.ProcessQueueInfo;
import org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.remoting.protocol.filter.FilterAPI;
import org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**消费者消费消息时，rocketmq内部实际干活的方法在这个类实现。。
 * 比如：消费者在启动的时候，start方法的主要逻辑是在这个类的start方法完成*/
public class DefaultMQPushConsumerImpl implements MQConsumerInner {
    /**
     * Delay some time when exception occur。。拉起消息时出现异常的延迟时间(比如：DefaultMQPushConsumerImpl的状态核对过程)
     */
    private long pullTimeDelayMillsWhenException = 3000;
    /**
     * Flow control interval when message cache is full..消息缓存已满的时候，延迟多久再拉取————理解为拉取消息时的流控参数
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL = 50;
    /**
     * Flow control interval when broker return flow control
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL = 20;
    /**
     * Delay some time when suspend pull service..如果当前消费者处于挂起状态时，延迟多久再拉取消息
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_SUSPEND = 1000;
    private static final long BROKER_SUSPEND_MAX_TIME_MILLIS = 1000 * 15;
    private static final long CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND = 1000 * 30;
    private static final Logger log = LoggerFactory.getLogger(DefaultMQPushConsumerImpl.class);
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    private final RebalanceImpl rebalanceImpl = new RebalancePushImpl(this);
    private final ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<>();
    private final long consumerStartTimestamp = System.currentTimeMillis();
    private final ArrayList<ConsumeMessageHook> consumeMessageHookList = new ArrayList<>();
    private final RPCHook rpcHook;
    private volatile ServiceState serviceState = ServiceState.CREATE_JUST;
    private MQClientInstance mQClientFactory;
    private PullAPIWrapper pullAPIWrapper;
    private volatile boolean pause = false;
    private boolean consumeOrderly = false;
    private MessageListener messageListenerInner;
    private OffsetStore offsetStore;
    private ConsumeMessageService consumeMessageService;
    private ConsumeMessageService consumeMessagePopService;
    private long queueFlowControlTimes = 0;
    private long queueMaxSpanFlowControlTimes = 0;

    //10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
    private final int[] popDelayLevel = new int[] {10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600, 7200};

    private static final int MAX_POP_INVISIBLE_TIME = 300000;
    private static final int MIN_POP_INVISIBLE_TIME = 5000;
    private static final int ASYNC_TIMEOUT = 3000;

    // only for test purpose, will be modified by reflection in unit test.
    @SuppressWarnings("FieldMayBeFinal")
    private static boolean doNotUpdateTopicSubscribeInfoWhenSubscriptionChanged = false;

    public DefaultMQPushConsumerImpl(DefaultMQPushConsumer defaultMQPushConsumer, RPCHook rpcHook) {
        this.defaultMQPushConsumer = defaultMQPushConsumer;
        this.rpcHook = rpcHook;
        this.pullTimeDelayMillsWhenException = defaultMQPushConsumer.getPullTimeDelayMillsWhenException();
    }

    public void registerFilterMessageHook(final FilterMessageHook hook) {
        this.filterMessageHookList.add(hook);
        log.info("register FilterMessageHook Hook, {}", hook.hookName());
    }

    public boolean hasHook() {
        return !this.consumeMessageHookList.isEmpty();
    }

    public void registerConsumeMessageHook(final ConsumeMessageHook hook) {
        this.consumeMessageHookList.add(hook);
        log.info("register consumeMessageHook Hook, {}", hook.hookName());
    }

    public void executeHookBefore(final ConsumeMessageContext context) {
        if (!this.consumeMessageHookList.isEmpty()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageBefore(context);
                } catch (Throwable e) {
                    log.warn("consumeMessageHook {} executeHookBefore exception", hook.hookName(), e);
                }
            }
        }
    }

    public void executeHookAfter(final ConsumeMessageContext context) {
        if (!this.consumeMessageHookList.isEmpty()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageAfter(context);
                } catch (Throwable e) {
                    log.warn("consumeMessageHook {} executeHookAfter exception", hook.hookName(), e);
                }
            }
        }
    }

    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }

    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.mQClientFactory.getMQAdminImpl().createTopic(key, newTopic, queueNum, topicSysFlag, null);
    }

    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        Set<MessageQueue> result = this.rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
        if (null == result) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            result = this.rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
        }

        if (null == result) {
            throw new MQClientException("The topic[" + topic + "] not exist", null);
        }

        return parseSubscribeMessageQueues(result);
    }

    public Set<MessageQueue> parseSubscribeMessageQueues(Set<MessageQueue> messageQueueList) {
        Set<MessageQueue> resultQueues = new HashSet<>();
        for (MessageQueue queue : messageQueueList) {
            String userTopic = NamespaceUtil.withoutNamespace(queue.getTopic(), this.defaultMQPushConsumer.getNamespace());
            resultQueues.add(new MessageQueue(userTopic, queue.getBrokerName(), queue.getQueueId()));
        }

        return resultQueues;
    }

    public DefaultMQPushConsumer getDefaultMQPushConsumer() {
        return defaultMQPushConsumer;
    }

    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().earliestMsgStoreTime(mq);
    }

    public long maxOffset(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
    }

    public long minOffset(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().minOffset(mq);
    }

    public OffsetStore getOffsetStore() {
        return offsetStore;
    }

    public void setOffsetStore(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }

    /**
     * 【拉取消息】主要就是校验 并且 计算拉取消息时 需要用到的参数
     * 【如何理解这个方法 和 PullAPIWrapper。pullKernelImpl、MQClientAPIImpl#pullMessage方法的区别】
     *      这个方法可以看成是纯rocketmq层面的功能，主要用于计算拉取消息时的所需要参数计算；
     *      pullKernelImpl方法则是rocketmq 和 netty 的中间层，主要的目的是 找到具体的broker地址 并且 构建请求头等信息
     *      MQClientAPIImpl#pullMessage方法则是调用netty的功能真正的拉取消息(用到了零拷贝的技术)
     * */
    public void pullMessage(final PullRequest pullRequest) {
        /**
         * 【疑问】意思是MessageRequest对应着不同的ProcessQueue？？这里先判断这个ProcessQueue已经失效了
         * step1：从PullRequest中获取ProcessQueue，如果处理队列当前状态未被丢弃，则更新ProcessQueue的
         *      lastPullTimestamp为当前时间戳。如果当前消费者被挂起，则将拉取任务延迟1s再放入
         *      PullMessageService的拉取任务队列中，最后结束本次消息拉取
         * 确保ProcessQueue、DefaultMQPushConsumerImpl的状态正常;更新ProcessQueue的最后拉取时
         *      间戳字段。。。对于中间的不正确现象抛出异常
         * */
        final ProcessQueue processQueue = pullRequest.getProcessQueue();
        if (processQueue.isDropped()) {
            log.info("the pull request[{}] is dropped.", pullRequest.toString());
            return;
        }

        pullRequest.getProcessQueue().setLastPullTimestamp(System.currentTimeMillis());

        try {
            this.makeSureStateOK(); //确保DefaultMQPushConsumerImpl的状态是ServiceState.RUNNING
        } catch (MQClientException e) {
            log.warn("pullMessage exception, consumer state not ok", e);
            //出现异常时，延迟3秒再拉取
            this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
            return;
        }
        /*如果被挂起，则将拉取任务延迟1s再放入PullMessageService的拉取任务队列中*/
        if (this.isPause()) {
            log.warn("consumer was paused, execute pull request later. instanceName={}, group={}", this.defaultMQPushConsumer.getInstanceName(), this.defaultMQPushConsumer.getConsumerGroup());
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_SUSPEND);
            return;
        }
        /**step2:查看缓存的消息的数量、总大小、消费间隔，然后判断是不是需要流控;*/
        long cachedMessageCount = processQueue.getMsgCount().get();
        long cachedMessageSizeInMiB = processQueue.getMsgSize().get() / (1024 * 1024);
        //缓存消息的数量超过阈值，则进行流控
        if (cachedMessageCount > this.defaultMQPushConsumer.getPullThresholdForQueue()) {
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL);
            if ((queueFlowControlTimes++ % 1000) == 0) {
                log.warn(
                    "the cached message count exceeds the threshold {}, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, pullRequest={}, flowControlTimes={}",
                    this.defaultMQPushConsumer.getPullThresholdForQueue(), processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), cachedMessageCount, cachedMessageSizeInMiB, pullRequest, queueFlowControlTimes);
            }
            return;
        }
        //缓存消息的大小超过阈值，则进行流控
        if (cachedMessageSizeInMiB > this.defaultMQPushConsumer.getPullThresholdSizeForQueue()) {
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL);
            if ((queueFlowControlTimes++ % 1000) == 0) {
                log.warn(
                    "the cached message size exceeds the threshold {} MiB, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, pullRequest={}, flowControlTimes={}",
                    this.defaultMQPushConsumer.getPullThresholdSizeForQueue(), processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), cachedMessageCount, cachedMessageSizeInMiB, pullRequest, queueFlowControlTimes);
            }
            return;
        }
        /*如果不是顺序消息：判断消息队列的跨度是否超过阈值，如果超过则进行流控(即非顺序类型消息多一个要求：待消
        费的消息(即ProcessQueue中的消息)的"最大偏移-最小偏移"超出2000的时候,会延迟50ms再拉取消息)
        processQueue.getMaxSpan()：计算出来消息队列的堆积情况(实质就是最大偏移 和 最小偏移之差)
        这一种情况主要的考量：担心因为一条消息堵塞，使消息进度无法向前推进，可能造成大量消息重复消费*/
        if (!this.consumeOrderly) {
            if (processQueue.getMaxSpan() > this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan()) {
                this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL);
                if ((queueMaxSpanFlowControlTimes++ % 1000) == 0) {
                    log.warn(
                        "the queue's messages, span too long, so do flow control, minOffset={}, maxOffset={}, maxSpan={}, pullRequest={}, flowControlTimes={}",
                        processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), processQueue.getMaxSpan(),
                        pullRequest, queueMaxSpanFlowControlTimes);
                }
                return;
            }
        } else { /**step3：计算或者更正待拉取消息的偏移量。如果上述的检查都通过。就需要更正下次拉取消息偏移量来拉取消息了*/
            if (processQueue.isLocked() /*将此标志置为true是在RebalanceImpl中*/) {
                /*如果这个请求是第一次执行锁定逻辑，则进入下面的if语句块————表明需要计算拉取偏移量*/
                if (!pullRequest.isPreviouslyLocked()) {
                    long offset = -1L;
                    /**注意下面的try-catch块，try中出现的异常会被catch块捕获。。同时如果offset<0，就会抛出异常中断方法执行*/
                    try {
                        /*计算当前的请求从哪里偏移量拉取消息*/
                        offset = this.rebalanceImpl.computePullFromWhereWithException(pullRequest.getMessageQueue());
                        if (offset < 0) {
                            throw new MQClientException(ResponseCode.SYSTEM_ERROR, "Unexpected offset " + offset);
                        }
                    } catch (Exception e) {
                        this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
                        log.error("Failed to compute pull offset, pullResult: {}", pullRequest, e);
                        return;
                    }
                    /**为什么需要判断这里？？？？*/
                    boolean brokerBusy = offset < pullRequest.getNextOffset();
                    log.info("the first time to pull message, so fix offset from broker. pullRequest: {} NewOffset: {} brokerBusy: {}",
                        pullRequest, offset, brokerBusy);
                    if (brokerBusy) {
                        log.info("[NOTIFYME]the first time to pull message, but pull request offset larger than broker consume offset. pullRequest: {} NewOffset: {}",
                            pullRequest, offset);
                    }
                    /*同一个PullRequest只会被执行一次锁定逻辑。注意与上面代码"if (!pullRequest.isPreviouslyLocked())"的呼应,上
                    面if条件的逻辑表明：如果不是第一次锁定，就没必要再走一次计算偏移的逻辑了*/
                    pullRequest.setPreviouslyLocked(true);
                    //更新下次拉取消息的偏移量 为 计算出的偏移量
                    pullRequest.setNextOffset(offset);
                }
            } else {
                this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
                log.info("pull message later because not locked in broker, {}", pullRequest);
                return;
            }
        }
        /**step4:根据topic来获取主题的订阅数据(封装为SubscriptionData对象)*/
        final MessageQueue messageQueue = pullRequest.getMessageQueue();
        final SubscriptionData subscriptionData = this.rebalanceImpl.getSubscriptionInner().get(messageQueue.getTopic());
        if (null == subscriptionData) {
            this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
            log.warn("find the consumer's subscription failed, {}", pullRequest);
            return;
        }

        final long beginTimestamp = System.currentTimeMillis();
        /**step5：构建拉取消息时的 回调(PullCallback)，拉取到消息后执行*/
        PullCallback pullCallback = new PullCallback() {
            @Override
            public void onSuccess(PullResult pullResult) {
                if (pullResult != null) {
                    //获取请求结果，反序列化成Message对象
                    pullResult = DefaultMQPushConsumerImpl.this.pullAPIWrapper.processPullResult(pullRequest.getMessageQueue(), pullResult,
                        subscriptionData);
                    /*判断响应状态，做不同的处理*/
                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            /*设置下一次拉取消息的起始偏移量*/
                            long prevRequestOffset = pullRequest.getNextOffset();
                            pullRequest.setNextOffset(pullResult.getNextBeginOffset());
                            /*记录消费者的本次统计信息————往返时间*/
                            long pullRT = System.currentTimeMillis() - beginTimestamp;
                            DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullRT(pullRequest.getConsumerGroup(),
                                pullRequest.getMessageQueue().getTopic(), pullRT);
                            /*如果msgFoundList为空，则立即将PullReqeuest放入PullMessageService的pullRequestQueue(这个流程就是
                            方法executePullRequestImmediately的逻辑)，以便PullMessageSerivce能及时唤醒并再次执行消息拉取*/
                            long firstMsgOffset = Long.MAX_VALUE;
                            if (pullResult.getMsgFoundList() == null || pullResult.getMsgFoundList().isEmpty()) {
                                DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest); //队列为空，立即拉取消息
                            } else {
                                firstMsgOffset = pullResult.getMsgFoundList().get(0).getQueueOffset();
                                /*记录消费者的统计信息————*/
                                DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullTPS(pullRequest.getConsumerGroup(),
                                    pullRequest.getMessageQueue().getTopic(), pullResult.getMsgFoundList().size());
                                //将消息放入ProcessQueue中，并提交给ConsumeMessageService进行消费
                                boolean dispatchToConsume = processQueue.putMessage(pullResult.getMsgFoundList());
                                DefaultMQPushConsumerImpl.this.consumeMessageService.submitConsumeRequest(
                                    pullResult.getMsgFoundList(),
                                    processQueue,
                                    pullRequest.getMessageQueue(),
                                    dispatchToConsume);

                                if (DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval() > 0) {
                                    DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest,
                                        DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval());
                                } else {
                                    //如果更新后的下次拉取等待时间<0，则立即拉取
                                    DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                                }
                            }

                            if (pullResult.getNextBeginOffset() < prevRequestOffset
                                || firstMsgOffset < prevRequestOffset) {
                                log.warn(
                                    "[BUG] pull message result maybe data wrong, nextBeginOffset: {} firstMsgOffset: {} prevRequestOffset: {}",
                                    pullResult.getNextBeginOffset(),
                                    firstMsgOffset,
                                    prevRequestOffset);
                            }

                            break;
                        case NO_NEW_MSG:
                        case NO_MATCHED_MSG:
                            pullRequest.setNextOffset(pullResult.getNextBeginOffset());

                            DefaultMQPushConsumerImpl.this.correctTagsOffset(pullRequest);

                            DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                            break;
                        case OFFSET_ILLEGAL:
                            log.warn("the pull request offset illegal, {} {}",
                                pullRequest.toString(), pullResult.toString());
                            pullRequest.setNextOffset(pullResult.getNextBeginOffset());

                            pullRequest.getProcessQueue().setDropped(true);
                            DefaultMQPushConsumerImpl.this.executeTask(new Runnable() {

                                @Override
                                public void run() {
                                    try {
                                        DefaultMQPushConsumerImpl.this.offsetStore.updateAndFreezeOffset(pullRequest.getMessageQueue(),
                                            pullRequest.getNextOffset());

                                        DefaultMQPushConsumerImpl.this.offsetStore.persist(pullRequest.getMessageQueue());

                                        // removeProcessQueue will also remove offset to cancel the frozen status.
                                        DefaultMQPushConsumerImpl.this.rebalanceImpl.removeProcessQueue(pullRequest.getMessageQueue());
                                        DefaultMQPushConsumerImpl.this.rebalanceImpl.getmQClientFactory().rebalanceImmediately();

                                        log.warn("fix the pull request offset, {}", pullRequest);
                                    } catch (Throwable e) {
                                        log.error("executeTaskLater Exception", e);
                                    }
                                }
                            });
                            break;
                        default:
                            break;
                    }
                }
            }

            @Override
            public void onException(Throwable e) {
                if (!pullRequest.getMessageQueue().getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    if (e instanceof MQBrokerException && ((MQBrokerException) e).getResponseCode() == ResponseCode.SUBSCRIPTION_NOT_LATEST) {
                        log.warn("the subscription is not latest, group={}, messageQueue={}", groupName(), messageQueue);
                    } else {
                        log.warn("execute the pull request exception, group={}, messageQueue={}", groupName(), messageQueue, e);
                    }
                }

                if (e instanceof MQBrokerException && ((MQBrokerException) e).getResponseCode() == ResponseCode.FLOW_CONTROL) {
                    DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL);
                } else {
                    DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
                }
            }
        };
        /**step6:计算commitOffsetValue。。。为啥只有clustering模式的时候采取内存读取偏移量(答：广播模式下消费进度由消费者
         *      本地管理，不需要提交给Broker)
         * 【】：消费者在向Broker发送拉取消息请求时，会先将客户端存储的消费进度提交到Broker端。————broker服务端关于此值的
         *      处理逻辑看方法tryCommitOffset*/
        boolean commitOffsetEnable = false;
        long commitOffsetValue = 0L;
        if (MessageModel.CLUSTERING == this.defaultMQPushConsumer.getMessageModel()) {
            commitOffsetValue = this.offsetStore.readOffset(pullRequest.getMessageQueue(), ReadOffsetType.READ_FROM_MEMORY);
            if (commitOffsetValue > 0) {
                commitOffsetEnable = true;
            }
        }
        /**step7:取出过滤表达式 subExpression；设置是不是类过滤模式*/
        String subExpression = null;
        boolean classFilter = false;
        SubscriptionData sd = this.rebalanceImpl.getSubscriptionInner().get(pullRequest.getMessageQueue().getTopic());
        if (sd != null) {
            /*为什么是两个条件的&&，为什么是这两个条件？？*/
            /*isPostSubscriptionWhenPull()：是不是每一次pull时，发送自己的订阅信息。
            * isClassFilterMode()：是不是类过滤模式*/
            if (this.defaultMQPushConsumer.isPostSubscriptionWhenPull() && !sd.isClassFilterMode()) {
                subExpression = sd.getSubString();
            }

            classFilter = sd.isClassFilterMode();
        }
        /**step8:构建系统标志 sysFlag*/
        int sysFlag = PullSysFlag.buildSysFlag(
            commitOffsetEnable, // commitOffset
            true, // suspend
            subExpression != null, // subscription
            classFilter // class filter
        );
        try {
            /**step9:调用PullAPIWrapper的方法与服务端交互 拉取消息*/
            this.pullAPIWrapper.pullKernelImpl(
                pullRequest.getMessageQueue(),
                subExpression,
                subscriptionData.getExpressionType(),
                subscriptionData.getSubVersion(),
                pullRequest.getNextOffset(),
                this.defaultMQPushConsumer.getPullBatchSize(),
                this.defaultMQPushConsumer.getPullBatchSizeInBytes(),
                sysFlag,
                commitOffsetValue,
                BROKER_SUSPEND_MAX_TIME_MILLIS,
                CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND,
                CommunicationMode.ASYNC, /*[note]拉取消息的时候采用的是异步的方式*/
                pullCallback
            );
        } catch (Exception e) {
            log.error("pullKernelImpl exception", e);
            this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException); //发生异常则稍后在拉取
        }
    }

    void popMessage(final PopRequest popRequest) {
        final PopProcessQueue processQueue = popRequest.getPopProcessQueue();
        if (processQueue.isDropped()) {
            log.info("the pop request[{}] is dropped.", popRequest.toString());
            return;
        }

        processQueue.setLastPopTimestamp(System.currentTimeMillis());

        try {
            this.makeSureStateOK();
        } catch (MQClientException e) {
            log.warn("pullMessage exception, consumer state not ok", e);
            this.executePopPullRequestLater(popRequest, pullTimeDelayMillsWhenException);
            return;
        }

        if (this.isPause()) {
            log.warn("consumer was paused, execute pull request later. instanceName={}, group={}", this.defaultMQPushConsumer.getInstanceName(), this.defaultMQPushConsumer.getConsumerGroup());
            this.executePopPullRequestLater(popRequest, PULL_TIME_DELAY_MILLS_WHEN_SUSPEND);
            return;
        }

        if (processQueue.getWaiAckMsgCount() > this.defaultMQPushConsumer.getPopThresholdForQueue()) {
            this.executePopPullRequestLater(popRequest, PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL);
            if ((queueFlowControlTimes++ % 1000) == 0) {
                log.warn("the messages waiting to ack exceeds the threshold {}, so do flow control, popRequest={}, flowControlTimes={}, wait count={}",
                    this.defaultMQPushConsumer.getPopThresholdForQueue(), popRequest, queueFlowControlTimes, processQueue.getWaiAckMsgCount());
            }
            return;
        }

        //POPTODO think of pop mode orderly implementation later.
        final SubscriptionData subscriptionData = this.rebalanceImpl.getSubscriptionInner().get(popRequest.getMessageQueue().getTopic());
        if (null == subscriptionData) {
            this.executePopPullRequestLater(popRequest, pullTimeDelayMillsWhenException);
            log.warn("find the consumer's subscription failed, {}", popRequest);
            return;
        }

        final long beginTimestamp = System.currentTimeMillis();

        PopCallback popCallback = new PopCallback() {
            @Override
            public void onSuccess(PopResult popResult) {
                if (popResult == null) {
                    log.error("pop callback popResult is null");
                    DefaultMQPushConsumerImpl.this.executePopPullRequestImmediately(popRequest);
                    return;
                }

                processPopResult(popResult, subscriptionData);

                switch (popResult.getPopStatus()) {
                    case FOUND:
                        long pullRT = System.currentTimeMillis() - beginTimestamp;
                        DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullRT(popRequest.getConsumerGroup(),
                            popRequest.getMessageQueue().getTopic(), pullRT);
                        if (popResult.getMsgFoundList() == null || popResult.getMsgFoundList().isEmpty()) {
                            DefaultMQPushConsumerImpl.this.executePopPullRequestImmediately(popRequest);
                        } else {
                            DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullTPS(popRequest.getConsumerGroup(),
                                popRequest.getMessageQueue().getTopic(), popResult.getMsgFoundList().size());
                            popRequest.getPopProcessQueue().incFoundMsg(popResult.getMsgFoundList().size());

                            DefaultMQPushConsumerImpl.this.consumeMessagePopService.submitPopConsumeRequest(
                                popResult.getMsgFoundList(),
                                processQueue,
                                popRequest.getMessageQueue());

                            if (DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval() > 0) {
                                DefaultMQPushConsumerImpl.this.executePopPullRequestLater(popRequest,
                                    DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval());
                            } else {
                                DefaultMQPushConsumerImpl.this.executePopPullRequestImmediately(popRequest);
                            }
                        }
                        break;
                    case NO_NEW_MSG:
                    case POLLING_NOT_FOUND:
                        DefaultMQPushConsumerImpl.this.executePopPullRequestImmediately(popRequest);
                        break;
                    case POLLING_FULL:
                        DefaultMQPushConsumerImpl.this.executePopPullRequestLater(popRequest, pullTimeDelayMillsWhenException);
                        break;
                    default:
                        DefaultMQPushConsumerImpl.this.executePopPullRequestLater(popRequest, pullTimeDelayMillsWhenException);
                        break;
                }

            }

            @Override
            public void onException(Throwable e) {
                if (!popRequest.getMessageQueue().getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    log.warn("execute the pull request exception: {}", e);
                }

                if (e instanceof MQBrokerException && ((MQBrokerException) e).getResponseCode() == ResponseCode.FLOW_CONTROL) {
                    DefaultMQPushConsumerImpl.this.executePopPullRequestLater(popRequest, PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL);
                } else {
                    DefaultMQPushConsumerImpl.this.executePopPullRequestLater(popRequest, pullTimeDelayMillsWhenException);
                }
            }
        };


        try {

            long invisibleTime = this.defaultMQPushConsumer.getPopInvisibleTime();
            if (invisibleTime < MIN_POP_INVISIBLE_TIME || invisibleTime > MAX_POP_INVISIBLE_TIME) {
                invisibleTime = 60000;
            }
            this.pullAPIWrapper.popAsync(popRequest.getMessageQueue(), invisibleTime, this.defaultMQPushConsumer.getPopBatchNums(),
                popRequest.getConsumerGroup(), BROKER_SUSPEND_MAX_TIME_MILLIS, popCallback, true, popRequest.getInitMode(),
                false, subscriptionData.getExpressionType(), subscriptionData.getSubString());
        } catch (Exception e) {
            log.error("popAsync exception", e);
            this.executePopPullRequestLater(popRequest, pullTimeDelayMillsWhenException);
        }
    }

    private PopResult processPopResult(final PopResult popResult, final SubscriptionData subscriptionData) {
        if (PopStatus.FOUND == popResult.getPopStatus()) {
            List<MessageExt> msgFoundList = popResult.getMsgFoundList();
            List<MessageExt> msgListFilterAgain = msgFoundList;
            if (!subscriptionData.getTagsSet().isEmpty() && !subscriptionData.isClassFilterMode()
                && popResult.getMsgFoundList().size() > 0) {
                msgListFilterAgain = new ArrayList<>(popResult.getMsgFoundList().size());
                for (MessageExt msg : popResult.getMsgFoundList()) {
                    if (msg.getTags() != null) {
                        if (subscriptionData.getTagsSet().contains(msg.getTags())) {
                            msgListFilterAgain.add(msg);
                        }
                    }
                }
            }

            if (!this.filterMessageHookList.isEmpty()) {
                FilterMessageContext filterMessageContext = new FilterMessageContext();
                filterMessageContext.setUnitMode(this.defaultMQPushConsumer.isUnitMode());
                filterMessageContext.setMsgList(msgListFilterAgain);
                if (!this.filterMessageHookList.isEmpty()) {
                    for (FilterMessageHook hook : this.filterMessageHookList) {
                        try {
                            hook.filterMessage(filterMessageContext);
                        } catch (Throwable e) {
                            log.error("execute hook error. hookName={}", hook.hookName());
                        }
                    }
                }
            }

            if (msgFoundList.size() != msgListFilterAgain.size()) {
                for (MessageExt msg : msgFoundList) {
                    if (!msgListFilterAgain.contains(msg)) {
                        ackAsync(msg, this.groupName());
                    }
                }
            }

            popResult.setMsgFoundList(msgListFilterAgain);
        }

        return popResult;
    }

    private void makeSureStateOK() throws MQClientException {
        if (this.serviceState != ServiceState.RUNNING) {
            throw new MQClientException("The consumer service state not OK, "
                + this.serviceState
                + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                null);
        }
    }

    void executePullRequestLater(final PullRequest pullRequest, final long timeDelay) {
        this.mQClientFactory.getPullMessageService().executePullRequestLater(pullRequest, timeDelay);
    }

    public boolean isPause() {
        return pause;
    }

    public void setPause(boolean pause) {
        this.pause = pause;
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.mQClientFactory.getConsumerStatsManager();
    }

    public void executePullRequestImmediately(final PullRequest pullRequest) {
        this.mQClientFactory.getPullMessageService().executePullRequestImmediately(pullRequest);
    }

    void executePopPullRequestLater(final PopRequest pullRequest, final long timeDelay) {
        this.mQClientFactory.getPullMessageService().executePopPullRequestLater(pullRequest, timeDelay);
    }

    void executePopPullRequestImmediately(final PopRequest pullRequest) {
        this.mQClientFactory.getPullMessageService().executePopPullRequestImmediately(pullRequest);
    }

    private void correctTagsOffset(final PullRequest pullRequest) {
        if (0L == pullRequest.getProcessQueue().getMsgCount().get()) {
            this.offsetStore.updateOffset(pullRequest.getMessageQueue(), pullRequest.getNextOffset(), true);
        }
    }

    public void executeTaskLater(final Runnable r, final long timeDelay) {
        this.mQClientFactory.getPullMessageService().executeTaskLater(r, timeDelay);
    }

    public void executeTask(final Runnable r) {
        this.mQClientFactory.getPullMessageService().executeTask(r);
    }

    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
        throws MQClientException, InterruptedException {
        return this.mQClientFactory.getMQAdminImpl().queryMessage(topic, key, maxNum, begin, end);
    }

    public MessageExt queryMessageByUniqKey(String topic, String uniqKey) throws MQClientException,
        InterruptedException {
        return this.mQClientFactory.getMQAdminImpl().queryMessageByUniqKey(topic, uniqKey);
    }

    public void registerMessageListener(MessageListener messageListener) {
        this.messageListenerInner = messageListener;
    }

    public void resume() {
        this.pause = false;
        doRebalance();
        log.info("resume this consumer, {}", this.defaultMQPushConsumer.getConsumerGroup());
    }

    @Deprecated
    public void sendMessageBack(MessageExt msg, int delayLevel, final String brokerName)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        sendMessageBack(msg, delayLevel, brokerName, null);
    }

    public void sendMessageBack(MessageExt msg, int delayLevel, final MessageQueue mq)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        sendMessageBack(msg, delayLevel, null, mq);
    }


    private void sendMessageBack(MessageExt msg, int delayLevel, final String brokerName, final MessageQueue mq)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        boolean needRetry = true;
        try {
            if (brokerName != null && brokerName.startsWith(MixAll.LOGICAL_QUEUE_MOCK_BROKER_PREFIX)
                || mq != null && mq.getBrokerName().startsWith(MixAll.LOGICAL_QUEUE_MOCK_BROKER_PREFIX)) {
                needRetry = false;
                sendMessageBackAsNormalMessage(msg);
            } else {
                String brokerAddr = (null != brokerName) ? this.mQClientFactory.findBrokerAddressInPublish(brokerName)
                    : RemotingHelper.parseSocketAddressAddr(msg.getStoreHost());
                this.mQClientFactory.getMQClientAPIImpl().consumerSendMessageBack(brokerAddr, brokerName, msg,
                    this.defaultMQPushConsumer.getConsumerGroup(), delayLevel, 5000, getMaxReconsumeTimes());
            }
        } catch (Throwable t) {
            log.error("Failed to send message back, consumerGroup={}, brokerName={}, mq={}, message={}",
                this.defaultMQPushConsumer.getConsumerGroup(), brokerName, mq, msg, t);
            if (needRetry) {
                sendMessageBackAsNormalMessage(msg);
            }
        } finally {
            msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQPushConsumer.getNamespace()));
        }
    }

    private void sendMessageBackAsNormalMessage(MessageExt msg) throws  RemotingException, MQBrokerException, InterruptedException, MQClientException {
        Message newMsg = new Message(MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup()), msg.getBody());

        String originMsgId = MessageAccessor.getOriginMessageId(msg);
        MessageAccessor.setOriginMessageId(newMsg, UtilAll.isBlank(originMsgId) ? msg.getMsgId() : originMsgId);

        newMsg.setFlag(msg.getFlag());
        MessageAccessor.setProperties(newMsg, msg.getProperties());
        MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
        MessageAccessor.setReconsumeTime(newMsg, String.valueOf(msg.getReconsumeTimes() + 1));
        MessageAccessor.setMaxReconsumeTimes(newMsg, String.valueOf(getMaxReconsumeTimes()));
        MessageAccessor.clearProperty(newMsg, MessageConst.PROPERTY_TRANSACTION_PREPARED);
        newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());

        this.mQClientFactory.getDefaultMQProducer().send(newMsg);
    }

    void ackAsync(MessageExt message, String consumerGroup) {
        final String extraInfo = message.getProperty(MessageConst.PROPERTY_POP_CK);

        try {
            String[] extraInfoStrs = ExtraInfoUtil.split(extraInfo);
            String brokerName = ExtraInfoUtil.getBrokerName(extraInfoStrs);
            int queueId = ExtraInfoUtil.getQueueId(extraInfoStrs);
            long queueOffset = ExtraInfoUtil.getQueueOffset(extraInfoStrs);
            String topic = message.getTopic();

            String desBrokerName = brokerName;
            if (brokerName != null && brokerName.startsWith(MixAll.LOGICAL_QUEUE_MOCK_BROKER_PREFIX)) {
                desBrokerName = this.mQClientFactory.getBrokerNameFromMessageQueue(this.defaultMQPushConsumer.queueWithNamespace(new MessageQueue(topic, brokerName, queueId)));
            }


            FindBrokerResult
                findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(desBrokerName, MixAll.MASTER_ID, true);
            if (null == findBrokerResult) {
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
                findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(desBrokerName, MixAll.MASTER_ID, true);
            }

            if (findBrokerResult == null) {
                log.error("The broker[" + desBrokerName + "] not exist");
                return;
            }

            AckMessageRequestHeader requestHeader = new AckMessageRequestHeader();
            requestHeader.setTopic(ExtraInfoUtil.getRealTopic(extraInfoStrs, topic, consumerGroup));
            requestHeader.setQueueId(queueId);
            requestHeader.setOffset(queueOffset);
            requestHeader.setConsumerGroup(consumerGroup);
            requestHeader.setExtraInfo(extraInfo);
            requestHeader.setBrokerName(brokerName);
            this.mQClientFactory.getMQClientAPIImpl().ackMessageAsync(findBrokerResult.getBrokerAddr(), ASYNC_TIMEOUT, new AckCallback() {
                @Override
                public void onSuccess(AckResult ackResult) {
                    if (ackResult != null && !AckStatus.OK.equals(ackResult.getStatus())) {
                        log.warn("Ack message fail. ackResult: {}, extraInfo: {}", ackResult, extraInfo);
                    }
                }
                @Override
                public void onException(Throwable e) {
                    log.warn("Ack message fail. extraInfo: {}  error message: {}", extraInfo, e.toString());
                }
            }, requestHeader);

        } catch (Throwable t) {
            log.error("ack async error.", t);
        }
    }

    void changePopInvisibleTimeAsync(String topic, String consumerGroup, String extraInfo, long invisibleTime, AckCallback callback)
        throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        String[] extraInfoStrs = ExtraInfoUtil.split(extraInfo);
        String brokerName = ExtraInfoUtil.getBrokerName(extraInfoStrs);
        int queueId = ExtraInfoUtil.getQueueId(extraInfoStrs);

        String desBrokerName = brokerName;
        if (brokerName != null && brokerName.startsWith(MixAll.LOGICAL_QUEUE_MOCK_BROKER_PREFIX)) {
            desBrokerName = this.mQClientFactory.getBrokerNameFromMessageQueue(this.defaultMQPushConsumer.queueWithNamespace(new MessageQueue(topic, brokerName, queueId)));
        }

        FindBrokerResult
            findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(desBrokerName, MixAll.MASTER_ID, true);
        if (null == findBrokerResult) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(desBrokerName, MixAll.MASTER_ID, true);
        }
        if (findBrokerResult != null) {
            ChangeInvisibleTimeRequestHeader requestHeader = new ChangeInvisibleTimeRequestHeader();
            requestHeader.setTopic(ExtraInfoUtil.getRealTopic(extraInfoStrs, topic, consumerGroup));
            requestHeader.setQueueId(queueId);
            requestHeader.setOffset(ExtraInfoUtil.getQueueOffset(extraInfoStrs));
            requestHeader.setConsumerGroup(consumerGroup);
            requestHeader.setExtraInfo(extraInfo);
            requestHeader.setInvisibleTime(invisibleTime);
            requestHeader.setBrokerName(brokerName);
            //here the broker should be polished
            this.mQClientFactory.getMQClientAPIImpl().changeInvisibleTimeAsync(brokerName, findBrokerResult.getBrokerAddr(), requestHeader, ASYNC_TIMEOUT, callback);
            return;
        }
        throw new MQClientException("The broker[" + desBrokerName + "] not exist", null);
    }

    public int getMaxReconsumeTimes() {
        // default reconsume times: 16
        if (this.defaultMQPushConsumer.getMaxReconsumeTimes() == -1) {
            return 16;
        } else {
            return this.defaultMQPushConsumer.getMaxReconsumeTimes();
        }
    }

    public void shutdown() {
        shutdown(0);
    }

    public synchronized void shutdown(long awaitTerminateMillis) {
        switch (this.serviceState) {
            case CREATE_JUST:
                break;
            case RUNNING:
                this.consumeMessageService.shutdown(awaitTerminateMillis);
                this.persistConsumerOffset();
                this.mQClientFactory.unregisterConsumer(this.defaultMQPushConsumer.getConsumerGroup());
                this.mQClientFactory.shutdown();
                log.info("the consumer [{}] shutdown OK", this.defaultMQPushConsumer.getConsumerGroup());
                this.rebalanceImpl.destroy();
                this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                break;
            case SHUTDOWN_ALREADY:
                break;
            default:
                break;
        }
    }

    /**【功能】DefaultMQPushConsumer的启动，最核心的逻辑在于DefaultMQPushConsumerImpl的start()方法
     * 【干的事】
     *      1.修改状态标志 并 检查DefaultMQPushConsumer类中可以供用户设置的多个字段
     *      2.copy一份订阅信息数据到RebalanceImpl
     *      3.如果是cluster集群模式，修改ClientConfig.instanceName
     *      4.
     *
     * */
    public synchronized void start() throws MQClientException {
        switch (this.serviceState) {
            case CREATE_JUST:
                log.info("the consumer [{}] start beginning. messageModel={}, isUnitMode={}", this.defaultMQPushConsumer.getConsumerGroup(),
                    this.defaultMQPushConsumer.getMessageModel(), this.defaultMQPushConsumer.isUnitMode());
                this.serviceState = ServiceState.START_FAILED;
                /*检查多种参数：消费者组名、defaultMQPushConsumer实例中的多个参数*/
                this.checkConfig();
                //订阅关系配置信息进行复制
                this.copySubscription();
                /*如果当前消费模式是 CLUSTERING，则将实例名设置为PID.
                    原因思考：集群模式下，同一个消息只能被消费者组内的一个消费者消费，为了保证消费者的唯一，将消费者的实例
                    名设置为PID+纳秒
                    说明：这里的changeInstanceNameToPID是ClientConfig类的，
                * */
                if (this.defaultMQPushConsumer.getMessageModel() == MessageModel.CLUSTERING) {
                    this.defaultMQPushConsumer.changeInstanceNameToPID();
                }
                //创建 或者 获取 MQClientInstance实例
                this.mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(this.defaultMQPushConsumer, this.rpcHook);
                /*为 消费者负载均衡实现rebalanceImpl 设置属性*/
                //①设置消费者组
                this.rebalanceImpl.setConsumerGroup(this.defaultMQPushConsumer.getConsumerGroup());
                //②设置消费模式
                this.rebalanceImpl.setMessageModel(this.defaultMQPushConsumer.getMessageModel());
                //③设置队列分配策略
                this.rebalanceImpl.setAllocateMessageQueueStrategy(this.defaultMQPushConsumer.getAllocateMessageQueueStrategy());
                //④设置当前的MQClientInstance实例
                this.rebalanceImpl.setmQClientFactory(this.mQClientFactory);
                /*获取pull的API包装，并 在pullAPIWrapper中注册消息过滤钩子*/
                if (this.pullAPIWrapper == null) {
                    this.pullAPIWrapper = new PullAPIWrapper(
                        mQClientFactory,
                        this.defaultMQPushConsumer.getConsumerGroup(), isUnitMode()); /*isUnitMode()的作用不是很清楚*/
                }
                this.pullAPIWrapper.registerFilterMessageHook(filterMessageHookList);
                /*下面的if-else语句块完成字段offsetStore的设置，设置后调用offsetStore的load方法。。。
                * 完成load方法后就会完成OffsetStore的offsetTable设置————offsetTable是一个map，它的key为MessageQueue，value为
                *       对应的消费偏移量(即这个消息队列消费到哪里了)
                * */
                if (this.defaultMQPushConsumer.getOffsetStore() != null) { //如果offsetStore不为空则使用当前的offsetStore方式
                    this.offsetStore = this.defaultMQPushConsumer.getOffsetStore();
                } else { //否则根据消费方式选择具体的offsetStore方式存储offset
                    switch (this.defaultMQPushConsumer.getMessageModel()) {
                        case BROADCASTING: //如果是广播模式，则使用本地文件存储offset————广播模式 偏移存储在本地文件中
                            this.offsetStore = new LocalFileOffsetStore(this.mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup());
                            break;
                        case CLUSTERING: //如果是集群模式，则使用broker存储offset————集群模式下，偏移存储在broker中
                            this.offsetStore = new RemoteBrokerOffsetStore(this.mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup());
                            break;
                        default:
                            break;
                    }
                    this.defaultMQPushConsumer.setOffsetStore(this.offsetStore); //设置offsetStore
                }
                this.offsetStore.load();
                /*
                * 下面的if-else：根据MessageListener的具体实现方式选取具体的消息拉取线程实现。。设置完
                *       成后调用服务的start方法
                * */
                //if: 如果消息监听器是MessageListenerOrderly，消费消息服务选择使用ConsumeMessageOrderlyService
                if (this.getMessageListenerInner() instanceof MessageListenerOrderly) {
                    this.consumeOrderly = true;
                    this.consumeMessageService =
                        new ConsumeMessageOrderlyService(this, (MessageListenerOrderly) this.getMessageListenerInner());
                    //POPTODO reuse Executor ?
                    this.consumeMessagePopService = new ConsumeMessagePopOrderlyService(this, (MessageListenerOrderly) this.getMessageListenerInner());
                } else if (this.getMessageListenerInner() instanceof MessageListenerConcurrently) {
                    //else if：如果消息监听器是MessageListenerConcurrently，消费消息服务选择使用ConsumeMessageConcurrentlyService
                    this.consumeOrderly = false;
                    this.consumeMessageService =
                        new ConsumeMessageConcurrentlyService(this, (MessageListenerConcurrently) this.getMessageListenerInner());
                    //POPTODO reuse Executor ?
                    this.consumeMessagePopService =
                        new ConsumeMessagePopConcurrentlyService(this, (MessageListenerConcurrently) this.getMessageListenerInner());
                }

                this.consumeMessageService.start();
                // POPTODO
                this.consumeMessagePopService.start();
                /*向MQClientInstance注册消费者，并 启动MQClientInstance(如果注册成功)；记录日志；设置状态*/
                boolean registerOK = mQClientFactory.registerConsumer(this.defaultMQPushConsumer.getConsumerGroup(), this);
                if (!registerOK) { //注册失败时
                    this.serviceState = ServiceState.CREATE_JUST;
                    this.consumeMessageService.shutdown(defaultMQPushConsumer.getAwaitTerminationMillisWhenShutdown());
                    throw new MQClientException("The consumer group[" + this.defaultMQPushConsumer.getConsumerGroup()
                        + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL),
                        null);
                }

                mQClientFactory.start();
                log.info("the consumer [{}] start OK.", this.defaultMQPushConsumer.getConsumerGroup());
                this.serviceState = ServiceState.RUNNING;
                break;
                /*
                * 【总述】除了CREATE_JUST，其他状态均抛出MQClientException异常，并记录日志。
                * 如果MQClientInstance已经启动，或者已经关闭，或者启动失败，重复调用start会报错。这里也能直
                *       观的反映出：MQClientInstance的启动只有一次
                * */
            case RUNNING:
            case START_FAILED:
            case SHUTDOWN_ALREADY:
                throw new MQClientException("The PushConsumer service state not OK, maybe started once, "
                    + this.serviceState
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                    null);
            default:
                break;
        }
        // 拿出订阅信息表中的所有topic，更新这些topic对应的路由信息
        this.updateTopicSubscribeInfoWhenSubscriptionChanged();
        //检查客户端状态
        this.mQClientFactory.checkClientInBroker();
        if (this.mQClientFactory.sendHeartbeatToAllBrokerWithLock()) { //向所有的broker发送心跳包，成功时进入if块
            this.mQClientFactory.rebalanceImmediately(); //唤醒 消费者负载均衡逻辑 的执行
        }
    }

    /**在DefaultMQPushConsumer中用户可以进行多个属性的(用户自定义)设置，这里就是对多个属性的合法性检查*/
    private void checkConfig() throws MQClientException {
        Validators.checkGroup(this.defaultMQPushConsumer.getConsumerGroup()); /*检查消费者组是否合法*/

        if (null == this.defaultMQPushConsumer.getConsumerGroup()) {
            throw new MQClientException(
                "consumerGroup is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }
        /*组名不能等同内部的字段DEFAULT_CONSUMER_GROUP*/
        if (this.defaultMQPushConsumer.getConsumerGroup().equals(MixAll.DEFAULT_CONSUMER_GROUP)) {
            throw new MQClientException(
                "consumerGroup can not equal "
                    + MixAll.DEFAULT_CONSUMER_GROUP
                    + ", please specify another one."
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }
        /*检查消息消费的模式，不能是null。。这个在声明字段的时候就初始化了*/
        if (null == this.defaultMQPushConsumer.getMessageModel()) {
            throw new MQClientException(
                "messageModel is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        if (null == this.defaultMQPushConsumer.getConsumeFromWhere()) {
            throw new MQClientException(
                "consumeFromWhere is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }
        //解析时间戳字段，如果出现异常，则抛出
        Date dt = UtilAll.parseDate(this.defaultMQPushConsumer.getConsumeTimestamp(), UtilAll.YYYYMMDDHHMMSS);
        if (null == dt) {
            throw new MQClientException(
                "consumeTimestamp is invalid, the valid format is yyyyMMddHHmmss,but received "
                    + this.defaultMQPushConsumer.getConsumeTimestamp()
                    + " " + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
        }

        // allocateMessageQueueStrategy；消息分配策略不能是空，默认的是平均分配
        if (null == this.defaultMQPushConsumer.getAllocateMessageQueueStrategy()) {
            throw new MQClientException(
                "allocateMessageQueueStrategy is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // subscription
        if (null == this.defaultMQPushConsumer.getSubscription()) {
            throw new MQClientException(
                "subscription is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        /* messageListener；消息监听器，在启动消费者时，必须设置消息监听器，否则会报错。官方例子
        的Consumer中设置了。。并且在DefaultMQPushConsumer.registerMessageListener中设置的监听
        器，会在方法内同时将监听器设置到DefaultMQPushConsumerImpl中
         */
        if (null == this.defaultMQPushConsumer.getMessageListener()) {
            throw new MQClientException(
                "messageListener is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }
        /*检查监听器的类型：要麽是orderly要麽是concurrently，如果都不是抛出异常*/
        boolean orderly = this.defaultMQPushConsumer.getMessageListener() instanceof MessageListenerOrderly;
        boolean concurrently = this.defaultMQPushConsumer.getMessageListener() instanceof MessageListenerConcurrently;
        if (!orderly && !concurrently) {
            throw new MQClientException(
                "messageListener must be instanceof MessageListenerOrderly or MessageListenerConcurrently"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // consumeThreadMin。检查消费者线程的最小值，是不是在[1,1000]区间，超出此区间抛出异常
        if (this.defaultMQPushConsumer.getConsumeThreadMin() < 1
            || this.defaultMQPushConsumer.getConsumeThreadMin() > 1000) {
            throw new MQClientException(
                "consumeThreadMin Out of range [1, 1000]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // consumeThreadMax：检查消费者线程的最小值，是不是在[1,1000]区间，超出此区间抛出异常
        if (this.defaultMQPushConsumer.getConsumeThreadMax() < 1 || this.defaultMQPushConsumer.getConsumeThreadMax() > 1000) {
            throw new MQClientException(
                "consumeThreadMax Out of range [1, 1000]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // consumeThreadMin can't be larger than consumeThreadMax：检查最大值>=最小值
        if (this.defaultMQPushConsumer.getConsumeThreadMin() > this.defaultMQPushConsumer.getConsumeThreadMax()) {
            throw new MQClientException(
                "consumeThreadMin (" + this.defaultMQPushConsumer.getConsumeThreadMin() + ") "
                    + "is larger than consumeThreadMax (" + this.defaultMQPushConsumer.getConsumeThreadMax() + ")",
                null);
        }

        // consumeConcurrentlyMaxSpan：检查字段DefaultMQPushConsumer.consumeConcurrentlyMaxSpan的值是不是符合要求
        if (this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan() < 1
            || this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan() > 65535) {
            throw new MQClientException(
                "consumeConcurrentlyMaxSpan Out of range [1, 65535]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // pullThresholdForQueue：队列级别的流量控制阈值，见DefaultMQPushConsumer.pullThresholdForQueue
        if (this.defaultMQPushConsumer.getPullThresholdForQueue() < 1 || this.defaultMQPushConsumer.getPullThresholdForQueue() > 65535) {
            throw new MQClientException(
                "pullThresholdForQueue Out of range [1, 65535]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // pullThresholdForTopic：主题级别的流量控制阈值，见DefaultMQPushConsumer.pullThresholdForTopic
        if (this.defaultMQPushConsumer.getPullThresholdForTopic() != -1) {
            if (this.defaultMQPushConsumer.getPullThresholdForTopic() < 1 || this.defaultMQPushConsumer.getPullThresholdForTopic() > 6553500) {
                throw new MQClientException(
                    "pullThresholdForTopic Out of range [1, 6553500]"
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
            }
        }

        // pullThresholdSizeForQueue：
        if (this.defaultMQPushConsumer.getPullThresholdSizeForQueue() < 1 || this.defaultMQPushConsumer.getPullThresholdSizeForQueue() > 1024) {
            throw new MQClientException(
                "pullThresholdSizeForQueue Out of range [1, 1024]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        if (this.defaultMQPushConsumer.getPullThresholdSizeForTopic() != -1) {
            // pullThresholdSizeForTopic
            if (this.defaultMQPushConsumer.getPullThresholdSizeForTopic() < 1 || this.defaultMQPushConsumer.getPullThresholdSizeForTopic() > 102400) {
                throw new MQClientException(
                    "pullThresholdSizeForTopic Out of range [1, 102400]"
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
            }
        }

        // pullInterval
        if (this.defaultMQPushConsumer.getPullInterval() < 0 || this.defaultMQPushConsumer.getPullInterval() > 65535) {
            throw new MQClientException(
                "pullInterval Out of range [0, 65535]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // consumeMessageBatchMaxSize
        if (this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize() < 1
            || this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize() > 1024) {
            throw new MQClientException(
                "consumeMessageBatchMaxSize Out of range [1, 1024]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // pullBatchSize
        if (this.defaultMQPushConsumer.getPullBatchSize() < 1 || this.defaultMQPushConsumer.getPullBatchSize() > 1024) {
            throw new MQClientException(
                "pullBatchSize Out of range [1, 1024]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // popInvisibleTime
        if (this.defaultMQPushConsumer.getPopInvisibleTime() < MIN_POP_INVISIBLE_TIME
            || this.defaultMQPushConsumer.getPopInvisibleTime() > MAX_POP_INVISIBLE_TIME) {
            throw new MQClientException(
                "popInvisibleTime Out of range [" + MIN_POP_INVISIBLE_TIME + ", " + MAX_POP_INVISIBLE_TIME + "]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // popBatchNums
        if (this.defaultMQPushConsumer.getPopBatchNums() <= 0 || this.defaultMQPushConsumer.getPopBatchNums() > 32) {
            throw new MQClientException(
                "popBatchNums Out of range [1, 32]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }
    }

    /**做了三件事()：
     * 复制DefaultMQPushConsumer#subscription(设置到this.rebalanceImpl的字段)；
     * 设置MessageListener；如果是集群消费模式还需要订阅重试队列；
     * 如果是cluster消费模式，还需要添加"重试队列"这种订阅信息(设置到this.rebalanceImpl的字段)；*/
    private void copySubscription() throws MQClientException {
        try {
            /*获取用户的订阅关系*/
            Map<String, String> sub = this.defaultMQPushConsumer.getSubscription();
            /*构建主题订阅消息SubscriptionData并加入RebalanceImpl的订阅消息中*/
            if (sub != null) {
                /**遍历DefaultMQPushConsumer#subscription，针对每一个键值对创建subscriptionData添加到RebalanceImpl#subscriptionInner中，
                 * 这里的逻辑就类似于subscribe方法*/
                for (final Map.Entry<String, String> entry : sub.entrySet()) { //遍历用户的订阅关系
                    final String topic = entry.getKey();
                    final String subString = entry.getValue();
                    SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, subString);
                    // 将订阅关系存储到内部订阅表中
                    this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
                }
            }
            // 为defaultMQPushConsumer设置具体的MessageListener实现
            if (null == this.messageListenerInner) {
                this.messageListenerInner = this.defaultMQPushConsumer.getMessageListener();
            }
            //根据集群消费模式，设置是否订阅重试主题消息
            switch (this.defaultMQPushConsumer.getMessageModel()) {
                case BROADCASTING: // 如果是广播消费模式，不处理，为什么？？？
                    break;
                case CLUSTERING: // 如果是集群消费模式，需要订阅重试主题消息 并且 设置订阅数据到再平衡实现rebalanceImpl中
                    //重试消息的topic名称：%RETRY% + 消费者组名
                    final String retryTopic = MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup());
                    SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(retryTopic, SubscriptionData.SUB_ALL);
                    this.rebalanceImpl.getSubscriptionInner().put(retryTopic, subscriptionData);
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    public MessageListener getMessageListenerInner() {
        return messageListenerInner;
    }

    /**【功能】更新订阅信息中 所有的topic的路由信息
     * 【逻辑】step1:根据getSubscriptionInner()方法拿到订阅关系表；step2:遍历表中的每一个key————即主题topic；
     *      step3:调用updateTopicRouteInfoFromNameServer方法更新该主题的路由信息
     * */
    private void updateTopicSubscribeInfoWhenSubscriptionChanged() {
        if (doNotUpdateTopicSubscribeInfoWhenSubscriptionChanged) {
            return;
        }
        /*获取内部订阅表，遍历表中的每一个主题，更新它的路由信息*/
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                final String topic = entry.getKey();
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            }
        }
    }

    public ConcurrentMap<String, SubscriptionData> getSubscriptionInner() {
        return this.rebalanceImpl.getSubscriptionInner();
    }

    /**第一种订阅方式：指定 topic 和 表达式(tag或者SQL92表达式都可以)
     * 【逻辑】将订阅信息添加到RebalanceImpl，以便进行消息队列负载均衡；订阅完成后需要向所有的Broker发送心跳包*/
    public void subscribe(String topic, String subExpression) throws MQClientException {
        try {
            SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, subExpression);
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            if (this.mQClientFactory != null) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    /**
     * 第二中订阅方式————
     * @param topic 要订阅的主题名称;
     * @Param fullClassName 全限定类名
     * @Param filterClassSource 过滤器类源代码
     * */
    public void subscribe(String topic, String fullClassName, String filterClassSource) throws MQClientException {
        try {
            SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, SubscriptionData.SUB_ALL);
            subscriptionData.setSubString(fullClassName);
            subscriptionData.setClassFilterMode(true);
            subscriptionData.setFilterClassSource(filterClassSource);
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            if (this.mQClientFactory != null) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            }

        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    public void subscribe(final String topic, final MessageSelector messageSelector) throws MQClientException {
        try {
            if (messageSelector == null) {
                subscribe(topic, SubscriptionData.SUB_ALL);
                return;
            }

            SubscriptionData subscriptionData = FilterAPI.build(topic,
                messageSelector.getExpression(), messageSelector.getExpressionType());

            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            if (this.mQClientFactory != null) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    public void suspend() {
        this.pause = true;
        log.info("suspend this consumer, {}", this.defaultMQPushConsumer.getConsumerGroup());
    }

    public void unsubscribe(String topic) {
        this.rebalanceImpl.getSubscriptionInner().remove(topic);
    }

    public void updateConsumeOffset(MessageQueue mq, long offset) {
        this.offsetStore.updateOffset(mq, offset, false);
    }

    public void updateCorePoolSize(int corePoolSize) {
        this.consumeMessageService.updateCorePoolSize(corePoolSize);
    }

    public MessageExt viewMessage(String msgId)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return this.mQClientFactory.getMQAdminImpl().viewMessage(msgId);
    }

    public RebalanceImpl getRebalanceImpl() {
        return rebalanceImpl;
    }

    public boolean isConsumeOrderly() {
        return consumeOrderly;
    }

    public void setConsumeOrderly(boolean consumeOrderly) {
        this.consumeOrderly = consumeOrderly;
    }

    public void resetOffsetByTimeStamp(long timeStamp) throws MQClientException {
        for (String topic : rebalanceImpl.getSubscriptionInner().keySet()) {
            Set<MessageQueue> mqs = rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
            if (CollectionUtils.isNotEmpty(mqs)) {
                Map<MessageQueue, Long> offsetTable = new HashMap<>(mqs.size(), 1);
                for (MessageQueue mq : mqs) {
                    long offset = searchOffset(mq, timeStamp);
                    offsetTable.put(mq, offset);
                }
                this.mQClientFactory.resetOffset(topic, groupName(), offsetTable);
            }
        }
    }

    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
    }

    @Override
    public String groupName() {
        return this.defaultMQPushConsumer.getConsumerGroup();
    }

    @Override
    public MessageModel messageModel() {
        return this.defaultMQPushConsumer.getMessageModel();
    }

    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_PASSIVELY;
    }

    @Override
    public ConsumeFromWhere consumeFromWhere() {
        return this.defaultMQPushConsumer.getConsumeFromWhere();
    }

    @Override
    public Set<SubscriptionData> subscriptions() {
        return new HashSet<>(this.rebalanceImpl.getSubscriptionInner().values());
    }

    @Override
    public void doRebalance() {
        if (!this.pause) {
            this.rebalanceImpl.doRebalance(this.isConsumeOrderly());
        }
    }

    @Override
    public boolean tryRebalance() {
        if (!this.pause) {
            return this.rebalanceImpl.doRebalance(this.isConsumeOrderly());
        }
        return false;
    }

    @Override
    public void persistConsumerOffset() {
        try {
            this.makeSureStateOK();
            Set<MessageQueue> mqs = new HashSet<>();
            Set<MessageQueue> allocateMq = this.rebalanceImpl.getProcessQueueTable().keySet();
            mqs.addAll(allocateMq);

            this.offsetStore.persistAll(mqs);
        } catch (Exception e) {
            log.error("group: " + this.defaultMQPushConsumer.getConsumerGroup() + " persistConsumerOffset exception", e);
        }
    }

    @Override
    public void updateTopicSubscribeInfo(String topic, Set<MessageQueue> info) {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                this.rebalanceImpl.topicSubscribeInfoTable.put(topic, info);
            }
        }
    }

    @Override
    public boolean isSubscribeTopicNeedUpdate(String topic) {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                return !this.rebalanceImpl.topicSubscribeInfoTable.containsKey(topic);
            }
        }

        return false;
    }

    @Override
    public boolean isUnitMode() {
        return this.defaultMQPushConsumer.isUnitMode();
    }

    @Override
    public ConsumerRunningInfo consumerRunningInfo() {
        ConsumerRunningInfo info = new ConsumerRunningInfo();

        Properties prop = MixAll.object2Properties(this.defaultMQPushConsumer);

        prop.put(ConsumerRunningInfo.PROP_CONSUME_ORDERLY, String.valueOf(this.consumeOrderly));
        prop.put(ConsumerRunningInfo.PROP_THREADPOOL_CORE_SIZE, String.valueOf(this.consumeMessageService.getCorePoolSize()));
        prop.put(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP, String.valueOf(this.consumerStartTimestamp));

        info.setProperties(prop);

        Set<SubscriptionData> subSet = this.subscriptions();
        info.getSubscriptionSet().addAll(subSet);

        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.rebalanceImpl.getProcessQueueTable().entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            MessageQueue mq = next.getKey();
            ProcessQueue pq = next.getValue();

            ProcessQueueInfo pqinfo = new ProcessQueueInfo();
            pqinfo.setCommitOffset(this.offsetStore.readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE));
            pq.fillProcessQueueInfo(pqinfo);
            info.getMqTable().put(mq, pqinfo);
        }

        Iterator<Entry<MessageQueue, PopProcessQueue>> popIt = this.rebalanceImpl.getPopProcessQueueTable().entrySet().iterator();
        while (popIt.hasNext()) {
            Entry<MessageQueue, PopProcessQueue> next = popIt.next();
            MessageQueue mq = next.getKey();
            PopProcessQueue pq = next.getValue();

            PopProcessQueueInfo pqinfo = new PopProcessQueueInfo();
            pq.fillPopProcessQueueInfo(pqinfo);
            info.getMqPopTable().put(mq, pqinfo);
        }

        for (SubscriptionData sd : subSet) {
            ConsumeStatus consumeStatus = this.mQClientFactory.getConsumerStatsManager().consumeStatus(this.groupName(), sd.getTopic());
            info.getStatusTable().put(sd.getTopic(), consumeStatus);
        }

        return info;
    }

    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }

    public void setmQClientFactory(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    public ServiceState getServiceState() {
        return serviceState;
    }

    //Don't use this deprecated setter, which will be removed soon.
    @Deprecated
    public synchronized void setServiceState(ServiceState serviceState) {
        this.serviceState = serviceState;
    }

    public void adjustThreadPool() {
        long computeAccTotal = this.computeAccumulationTotal();
        long adjustThreadPoolNumsThreshold = this.defaultMQPushConsumer.getAdjustThreadPoolNumsThreshold();

        long incThreshold = (long) (adjustThreadPoolNumsThreshold * 1.0);

        long decThreshold = (long) (adjustThreadPoolNumsThreshold * 0.8);

        if (computeAccTotal >= incThreshold) {
            this.consumeMessageService.incCorePoolSize();
        }

        if (computeAccTotal < decThreshold) {
            this.consumeMessageService.decCorePoolSize();
        }
    }

    private long computeAccumulationTotal() {
        long msgAccTotal = 0;
        ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = this.rebalanceImpl.getProcessQueueTable();
        Iterator<Entry<MessageQueue, ProcessQueue>> it = processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            ProcessQueue value = next.getValue();
            msgAccTotal += value.getMsgAccCnt();
        }

        return msgAccTotal;
    }

    public List<QueueTimeSpan> queryConsumeTimeSpan(final String topic)
        throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        List<QueueTimeSpan> queueTimeSpan = new ArrayList<>();
        TopicRouteData routeData = this.mQClientFactory.getMQClientAPIImpl().getTopicRouteInfoFromNameServer(topic, 3000);
        for (BrokerData brokerData : routeData.getBrokerDatas()) {
            String addr = brokerData.selectBrokerAddr();
            queueTimeSpan.addAll(this.mQClientFactory.getMQClientAPIImpl().queryConsumeTimeSpan(addr, topic, groupName(), 3000));
        }

        return queueTimeSpan;
    }

    public void tryResetPopRetryTopic(final List<MessageExt> msgs, String consumerGroup) {
        String popRetryPrefix = MixAll.RETRY_GROUP_TOPIC_PREFIX + consumerGroup + "_";
        for (MessageExt msg : msgs) {
            if (msg.getTopic().startsWith(popRetryPrefix)) {
                String normalTopic = KeyBuilder.parseNormalTopic(msg.getTopic(), consumerGroup);
                if (normalTopic != null && !normalTopic.isEmpty()) {
                    msg.setTopic(normalTopic);
                }
            }
        }
    }


    public void resetRetryAndNamespace(final List<MessageExt> msgs, String consumerGroup) {
        final String groupTopic = MixAll.getRetryTopic(consumerGroup);
        for (MessageExt msg : msgs) {
            String retryTopic = msg.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
            if (retryTopic != null && groupTopic.equals(msg.getTopic())) {
                msg.setTopic(retryTopic);
            }

            if (StringUtils.isNotEmpty(this.defaultMQPushConsumer.getNamespace())) {
                msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQPushConsumer.getNamespace()));
            }
        }
    }

    public ConsumeMessageService getConsumeMessageService() {
        return consumeMessageService;
    }

    public void setConsumeMessageService(ConsumeMessageService consumeMessageService) {
        this.consumeMessageService = consumeMessageService;

    }

    public void setPullTimeDelayMillsWhenException(long pullTimeDelayMillsWhenException) {
        this.pullTimeDelayMillsWhenException = pullTimeDelayMillsWhenException;
    }

    int[] getPopDelayLevel() {
        return popDelayLevel;
    }

    public MessageQueueListener getMessageQueueListener() {
        if (null == defaultMQPushConsumer) {
            return null;
        }
        return defaultMQPushConsumer.getMessageQueueListener();
    }
}
