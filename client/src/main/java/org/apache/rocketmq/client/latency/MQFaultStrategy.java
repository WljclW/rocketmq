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

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo.QueueFilter;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 实现消息的 故障转移 和 负载均衡
 * 【和LatencyFaultToleranceImpl的关系】
 *      MQFaultStrategy为消息的故障转移和负载均衡提供支持。在这个过程中，会调用到LatencyFaultToleranceImpl的方法来判断
 *      Broker的延迟信息 和 状态
 * */
public class MQFaultStrategy {
    private LatencyFaultTolerance<String> latencyFaultTolerance;
    /*是否启用 发送延迟故障规避策略 的一个布尔字段。它的主要作用是决定生产者在选择Broker时，是否需要考
    虑 Broker 的历史发送延迟情况，以避免将消息发送到性能较差或不可靠的 Broker。*/
    private volatile boolean sendLatencyFaultEnable;
    private volatile boolean startDetectorEnable;
    /**
     * RocketMQ为每个Broker预测了个可用时间(当前时间+notAvailableDuration)，当当前时间大于该时间，才
     *      代表Broker可用，而notAvailableDuration有6个级别和latencyMax的区间一一对应，根据传入
     *      的currentLatency去预测该Broker在什么时候可用。
     * 比如：currentLatency大于等于50小于100，则notAvailableDuration为0；
     *      currentLatency大于等于100小于550，则notAvailableDuration为0；
     *      currentLatency大于等于550小于1000，则notAvailableDuration为300000
     * */
    private long[] latencyMax = {50L, 100L, 550L, 1800L, 3000L, 5000L, 15000L};
    private long[] notAvailableDuration = {0L, 0L, 2000L, 5000L, 6000L, 10000L, 30000L};

    public static class BrokerFilter implements QueueFilter {
        private String lastBrokerName;

        public void setLastBrokerName(String lastBrokerName) {
            this.lastBrokerName = lastBrokerName;
        }

        @Override public boolean filter(MessageQueue mq) {
            if (lastBrokerName != null) {
                return !mq.getBrokerName().equals(lastBrokerName);
            }
            return true;
        }
    }

    /**定义了一个 ThreadLocal 变量 threadBrokerFilter，用于为每个线程提供独立的 BrokerFilter 实例。*/
    private ThreadLocal<BrokerFilter> threadBrokerFilter = new ThreadLocal<BrokerFilter>() {
        @Override protected BrokerFilter initialValue() {
            return new BrokerFilter();
        }
    };

    private QueueFilter reachableFilter = new QueueFilter() {
        @Override public boolean filter(MessageQueue mq) {
            return latencyFaultTolerance.isReachable(mq.getBrokerName());
        }
    };

    private QueueFilter availableFilter = new QueueFilter() {
        @Override public boolean filter(MessageQueue mq) {
            return latencyFaultTolerance.isAvailable(mq.getBrokerName());
        }
    };


    public MQFaultStrategy(ClientConfig cc, Resolver fetcher, ServiceDetector serviceDetector) {
        this.latencyFaultTolerance = new LatencyFaultToleranceImpl(fetcher, serviceDetector);
        this.latencyFaultTolerance.setDetectInterval(cc.getDetectInterval());
        this.latencyFaultTolerance.setDetectTimeout(cc.getDetectTimeout());
        this.setStartDetectorEnable(cc.isStartDetectorEnable());
        this.setSendLatencyFaultEnable(cc.isSendLatencyEnable());
    }

    // For unit test.
    public MQFaultStrategy(ClientConfig cc, LatencyFaultTolerance<String> tolerance) {
        this.setStartDetectorEnable(cc.isStartDetectorEnable());
        this.setSendLatencyFaultEnable(cc.isSendLatencyEnable());
        this.latencyFaultTolerance = tolerance;
        this.latencyFaultTolerance.setDetectInterval(cc.getDetectInterval());
        this.latencyFaultTolerance.setDetectTimeout(cc.getDetectTimeout());
    }


    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public QueueFilter getAvailableFilter() {
        return availableFilter;
    }

    public QueueFilter getReachableFilter() {
        return reachableFilter;
    }

    public ThreadLocal<BrokerFilter> getThreadBrokerFilter() {
        return threadBrokerFilter;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    public boolean isStartDetectorEnable() {
        return startDetectorEnable;
    }

    public void setStartDetectorEnable(boolean startDetectorEnable) {
        this.startDetectorEnable = startDetectorEnable;
        this.latencyFaultTolerance.setStartDetectorEnable(startDetectorEnable);
    }

    public void startDetector() {
        this.latencyFaultTolerance.startDetector();
    }

    public void shutdown() {
        this.latencyFaultTolerance.shutdown();
    }

    /**【】：根据sendLatencyFaultEnable的值来决定，靠哪些过滤器来筛选出一个消息队列。
     *      不管哪种方案，没有选到满足条件的messageQueue时，tpInfo.selectOneMessageQueue()是
     *      兜底方案。*/
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName, final boolean resetIndex) {
        BrokerFilter brokerFilter = threadBrokerFilter.get();
        brokerFilter.setLastBrokerName(lastBrokerName);
        //情况1：下面的if是故障检测的逻辑：如果开启的话，会根据所有过滤器(比如：可达不可达、brokerFilter)把根据mod拿到的消息队列筛一遍
        if (this.sendLatencyFaultEnable) {  //判断是否开启”延迟故障检测“，默认是false
            if (resetIndex) {
                tpInfo.resetIndex();
            }
            /*优先查找isAvailable的。判断逻辑(系统当前时间 不小于 startTimestamp属性的值)*/
            MessageQueue mq = tpInfo.selectOneMessageQueue(availableFilter, brokerFilter);
            if (mq != null) {
                return mq;
            }
            /*如果没有找到，再考虑可到达的isReachable。判断逻辑(看reachableFlag的值)*/
            mq = tpInfo.selectOneMessageQueue(reachableFilter, brokerFilter);
            if (mq != null) {
                return mq;
            }

            return tpInfo.selectOneMessageQueue();
        }
        /*情况2：如果关闭了"延迟故障检测"，则"只"根据brokerFilter过滤器把根据mod拿到的消息队列，
         用过滤器的规则筛一遍，并返回。*/
        MessageQueue mq = tpInfo.selectOneMessageQueue(brokerFilter);
        if (mq != null) {
            return mq;
        }
        return tpInfo.selectOneMessageQueue();
    }

    /**
     * []：根据参数更新FaultItem
     * @param brokerName brokerName（代表一个broker集群）
     * @param currentLatency 当前延迟值（代表发送消息所耗费的时间）
     * @param isolation isolation（代表是否隔离）???
     * @param reachable reachable（代表是否可用）*/
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation,
                                final boolean reachable) {
        if (this.sendLatencyFaultEnable) {
            /**
             * 首次isolation传入的是false，currentLatency是发送消息所耗费的时间————从开始发送消息到最后
             *      得到sendKernelImpl方法返回的sendResult经历的时间(换言之即发送这个消息的耗时)
             * */
            long duration = computeNotAvailableDuration(isolation ? 10000 : currentLatency);
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration, reachable);
        }
    }

    /**
     * 【】：根据当前的延迟值（currentLatency）计算一个不可用持续时间（notAvailableDuration）。
     * 逻辑实现：在latencyMax数组中找到不大于currentLatency的最大值假设索引是m，返回
     *      notAvailableDuration数组中这个下标(m)对应的值
     */
    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i]) {
                return this.notAvailableDuration[i];
            }
        }

        return 0;
    }
}
