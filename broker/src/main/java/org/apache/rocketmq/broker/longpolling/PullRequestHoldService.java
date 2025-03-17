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
package org.apache.rocketmq.broker.longpolling;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.ConsumeQueueExt;

/**[]：用于实现长轮询机制的核心服务(服务端长轮询消息拉取组件)。它的主要作用是处理消费者的拉取消息请求，并在消息未准备好时将请求
 *      挂起，直到有新消息到达或超时为止。这种机制能够有效减少无效的轮询请求，提高系统的性能和资源利用率。
 * 【】：
 * 1. RocketMQ 为了提高网络性能，在拉取消息时如果没有新消息，不会马上返回，而是会将该查询请求挂起一段时间，然后再重试
 *      查询。如果一直没有新消息，直到轮询时间超过设定的阈值才会返回。*/
public class PullRequestHoldService extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    protected static final String TOPIC_QUEUEID_SEPARATOR = "@";
    protected final BrokerController brokerController;
    //系统时钟
    private final SystemClock systemClock = new SystemClock();
    // 长轮询消息拉取请求表,key为topic@queueId,value为PullRequest集合
    protected ConcurrentMap<String/* topic@queueId */, ManyPullRequest /*是一个很多拉取请求的包装类*/> pullRequestTable =
        new ConcurrentHashMap<>(1024);

    public PullRequestHoldService(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**如果没有拉取到请求，就挂起拉取请求，等待当前线程长轮询处理*/
    public void suspendPullRequest(final String topic, final int queueId, final PullRequest pullRequest) {
        String key = this.buildKey(topic, queueId);
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (null == mpr) {
            mpr = new ManyPullRequest();
            ManyPullRequest prev = this.pullRequestTable.putIfAbsent(key, mpr);
            if (prev != null) {
                mpr = prev;
            }
        }

        pullRequest.getRequestCommand().setSuspended(true);
        mpr.addPullRequest(pullRequest);
    }

    private String buildKey(final String topic, final int queueId) {
        StringBuilder sb = new StringBuilder(topic.length() + 5);
        sb.append(topic);
        sb.append(TOPIC_QUEUEID_SEPARATOR);
        sb.append(queueId);
        return sb.toString();
    }

    /**[]:处理长轮询的核心逻辑
     * 【】：
     * 1. 如果开启长轮询，PullRequestHoldService线程会每隔5s被唤醒去尝试检测是否有新消息的到来直到超时，如果
     *      被挂起，需要等待5s，消息拉取实时性比较差。为避免这种情况，RocketMQ引I入另外一种机制：当消息到达时，
     *      唤醒挂起线程触发一次检查。*/
    @Override
    public void run() {
        log.info("{} service started", this.getServiceName());
        //逻辑就是 循环处理被挂起的"拉取消息的请求"
        while (!this.isStopped()) {
            try {
                if (this.brokerController.getBrokerConfig().isLongPollingEnable()) { //长轮询，默认5秒
                    this.waitForRunning(5 * 1000);
                } else { //短轮询，默认1秒
                    this.waitForRunning(this.brokerController.getBrokerConfig().getShortPollingTimeMills());
                }

                long beginLockTimestamp = this.systemClock.now();
                this.checkHoldRequest();
                long costTime = this.systemClock.now() - beginLockTimestamp;
                if (costTime > 5 * 1000) {
                    log.warn("PullRequestHoldService: check hold pull request cost {}ms", costTime);
                }
            } catch (Throwable e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        log.info("{} service end", this.getServiceName());
    }

    @Override
    public String getServiceName() {
        if (brokerController != null && brokerController.getBrokerConfig().isInBrokerContainer()) {
            return this.brokerController.getBrokerIdentity().getIdentifier() + PullRequestHoldService.class.getSimpleName();
        }
        return PullRequestHoldService.class.getSimpleName();
    }

    /**【】：检查是不是又可以处理的"拉取消息的请求"
     * 思路：遍历pullRequestTable，根据每一个键中的topic和queueId来获取该消息队列的偏移量，然后调用notifyMessageArriving看看是
     *      不是处理一些挂起的请求*/
    protected void checkHoldRequest() {
        for (String key : this.pullRequestTable.keySet()) {
            String[] kArray = key.split(TOPIC_QUEUEID_SEPARATOR);
            if (2 == kArray.length) {
                String topic = kArray[0];
                int queueId = Integer.parseInt(kArray[1]);
                // 从消息存储组件获取当前队列消费的最大偏移量————需要依靠主题(topic)以及queueId(消息队列id)这两个变量来获取
                final long offset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                try {
                    this.notifyMessageArriving(topic, queueId, offset);
                } catch (Throwable e) {
                    log.error(
                        "PullRequestHoldService: failed to check hold request failed, topic={}, queueId={}", topic,
                        queueId, e);
                }
            }
        }
    }

    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset) {
        notifyMessageArriving(topic, queueId, maxOffset, null, 0, null, null);
    }

    /**[]:
     * DefaultMessageStore在消息到达的时候会调用这个方法；PullRequestHoldService也会定期调用这个方法*/
    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset, final Long tagsCode,
        long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        String key = this.buildKey(topic, queueId);
        /*获取"topic@queueId"对应的ManyPullRequest————也就是说，这样的键值可能对应很多次请求，获取对应的所有请求封
            装的ManyPullRequest,ManyPullRequest类持有一个list，这个list存放着所有的拉取请求*/
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (mpr != null) {
            //克隆被挂起的拉取请求
            List<PullRequest> requestList = mpr.cloneListAndClear();
            if (requestList != null) {
                List<PullRequest> replayList = new ArrayList<>();

                for (PullRequest request : requestList) {
                    long newestOffset = maxOffset;
                    // 如果拉取请求对应的消息队列的最大偏移量 小于 当前拉取的起始偏移量，则再次查询该请求对应的消息队列的最大偏移量(更新newestOffset)。
                    if (newestOffset <= request.getPullFromThisOffset()) {
                        newestOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                    }
                    // 如果拉取请求对应的消息队列的最大偏移量 大于 当前拉取的起始偏移量(正常的情况)，则说明消息存储组件有消息，则唤醒请求
                    if (newestOffset > request.getPullFromThisOffset()) {
                        boolean match = request.getMessageFilter().isMatchedByConsumeQueue(tagsCode,
                            new ConsumeQueueExt.CqExtUnit(tagsCode, msgStoreTime, filterBitMap));
                        // match by bit map, need eval again when properties is not null.
                        if (match && properties != null) {
                            match = request.getMessageFilter().isMatchedByCommitLog(null, properties);
                        }

                        if (match) {
                            try {
                                /*
                                 * 这次不管有没有读取新消息，都会返回 reponse 给 consumer
                                 */
                                this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                                    request.getRequestCommand());
                            } catch (Throwable e) {
                                log.error(
                                    "PullRequestHoldService#notifyMessageArriving: failed to execute request when "
                                        + "message matched, topic={}, queueId={}", topic, queueId, e);
                            }
                            continue;
                        }
                    }
                    /*如果执行超时，*/
                    if (System.currentTimeMillis() >= (request.getSuspendTimestamp() + request.getTimeoutMillis())) {
                        try {
                            this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                                request.getRequestCommand());
                        } catch (Throwable e) {
                            log.error(
                                "PullRequestHoldService#notifyMessageArriving: failed to execute request when time's "
                                    + "up, topic={}, queueId={}", topic, queueId, e);
                        }
                        continue;
                    }

                    replayList.add(request);
                }
                //要重试的请求？？
                if (!replayList.isEmpty()) {
                    mpr.addPullRequest(replayList);
                }
            }
        }
    }

    public void notifyMasterOnline() {
        for (ManyPullRequest mpr : this.pullRequestTable.values()) {
            if (mpr == null || mpr.isEmpty()) {
                continue;
            }
            for (PullRequest request : mpr.cloneListAndClear()) {
                try {
                    log.info("notify master online, wakeup {} {}", request.getClientChannel(), request.getRequestCommand());
                    this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                        request.getRequestCommand());
                } catch (Throwable e) {
                    log.error("execute request when master online failed.", e);
                }
            }
        }

    }
}
