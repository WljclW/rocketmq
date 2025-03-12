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
package org.apache.rocketmq.broker.coldctr;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.fastjson.JSONObject;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.coldctr.AccAndTimeStamp;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.config.MessageStoreConfig;

/**
 * store the cg cold read ctr table and acc the size of the cold
 * reading msg, timing to clear the table and set acc to zero
 */
public class ColdDataCgCtrService extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_COLDCTR_LOGGER_NAME);
    private final SystemClock systemClock = new SystemClock();
    private final long cgColdAccResideTimeoutMills = 60 * 1000;
    private static final AtomicLong GLOBAL_ACC = new AtomicLong(0L);
    private static final String ADAPTIVE = "||adaptive";
    /**
     * as soon as the consumerGroup read the cold data then it will be put into @code cgColdThresholdMapRuntime,
     * and it also will be removed when does not read cold data in @code cgColdAccResideTimeoutMills later;
     * 一旦consumerGroup读取冷数据，这个消费者组会被加入到cgColdThresholdMapRuntime中；同时，如果该消费者组不再读取冷数据，
     *      在cgColdAccResideTimeoutMills之后，会从cgColdThresholdMapRuntime中移除————因此从上面的表述中可以发现这个清
     *      除操作应该在当前服务的run方法
     */
    private final ConcurrentHashMap<String, AccAndTimeStamp> cgColdThresholdMapRuntime = new ConcurrentHashMap<>();
    /**
     * if the system admin wants to set the special cold read threshold for some consumerGroup, the configuration will
     * be putted into @code cgColdThresholdMapConfig
     */
    private final ConcurrentHashMap<String, Long> cgColdThresholdMapConfig = new ConcurrentHashMap<>();
    private final BrokerConfig brokerConfig;
    private final MessageStoreConfig messageStoreConfig;
    private final ColdCtrStrategy coldCtrStrategy;

    public ColdDataCgCtrService(BrokerController brokerController) {
        this.brokerConfig = brokerController.getBrokerConfig();
        this.messageStoreConfig = brokerController.getMessageStoreConfig();
        this.coldCtrStrategy = brokerConfig.isUsePIDColdCtrStrategy() ? new PIDAdaptiveColdCtrStrategy(this, (long)(brokerConfig.getGlobalColdReadThreshold() * 0.8)) : new SimpleColdCtrStrategy(this);
    }

    @Override
    public String getServiceName() {
        return ColdDataCgCtrService.class.getSimpleName();
    }

    @Override
    public void run() {
        log.info("{} service started", this.getServiceName());
        while (!this.isStopped()) {
            try {
                if (messageStoreConfig.isColdDataFlowControlEnable()) {
                    this.waitForRunning(5 * 1000);
                } else {
                    this.waitForRunning(180 * 1000);
                }
                long beginLockTimestamp = this.systemClock.now();
                clearDataAcc();
                if (!brokerConfig.isColdCtrStrategyEnable()) {
                    clearAdaptiveConfig();
                }
                long costTime = this.systemClock.now() - beginLockTimestamp;
                log.info("[{}] clearTheDataAcc-cost {} ms.", costTime > 3 * 1000 ? "NOTIFYME" : "OK", costTime);
            } catch (Throwable e) {
                log.warn(this.getServiceName() + " service has exception", e);
            }
        }
        log.info("{} service end", this.getServiceName());
    }

    public String getColdDataFlowCtrInfo() {
        JSONObject result = new JSONObject();
        result.put("runtimeTable", this.cgColdThresholdMapRuntime);
        result.put("configTable", this.cgColdThresholdMapConfig);
        result.put("cgColdReadThreshold", this.brokerConfig.getCgColdReadThreshold());
        result.put("globalColdReadThreshold", this.brokerConfig.getGlobalColdReadThreshold());
        result.put("globalAcc", GLOBAL_ACC.get());
        return result.toJSONString();
    }

    /**
     * clear the long time no cold read cg in the table;
     * update the acc to zero for the cg in the table;
     * use the strategy to promote or decelerate the cg;
     */
    private void clearDataAcc() {
        log.info("clearDataAcc cgColdThresholdMapRuntime key size: {}", cgColdThresholdMapRuntime.size());
        if (brokerConfig.isColdCtrStrategyEnable()) {
            coldCtrStrategy.collect(GLOBAL_ACC.get());
        }
        Iterator<Entry<String, AccAndTimeStamp>> iterator = cgColdThresholdMapRuntime.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<String, AccAndTimeStamp> next = iterator.next();
            if (System.currentTimeMillis() >= cgColdAccResideTimeoutMills + next.getValue().getLastColdReadTimeMills()) {
                if (brokerConfig.isColdCtrStrategyEnable()) {
                    cgColdThresholdMapConfig.remove(buildAdaptiveKey(next.getKey()));
                }
                iterator.remove();
            } else if (next.getValue().getColdAcc().get() >= getThresholdByConsumerGroup(next.getKey())) {
                log.info("Coldctr consumerGroup: {}, acc: {}, threshold: {}", next.getKey(), next.getValue().getColdAcc().get(), getThresholdByConsumerGroup(next.getKey()));
                if (brokerConfig.isColdCtrStrategyEnable() && !isGlobalColdCtr() && !isAdminConfig(next.getKey())) {
                    coldCtrStrategy.promote(buildAdaptiveKey(next.getKey()), getThresholdByConsumerGroup(next.getKey()));
                }
            }
            next.getValue().getColdAcc().set(0L);
        }
        if (isGlobalColdCtr()) {
            log.info("Coldctr global acc: {}, threshold: {}", GLOBAL_ACC.get(), this.brokerConfig.getGlobalColdReadThreshold());
        }
        if (brokerConfig.isColdCtrStrategyEnable()) {
            sortAndDecelerate();
        }
        GLOBAL_ACC.set(0L);
    }

    private void sortAndDecelerate() {
        List<Entry<String, Long>> configMapList = new ArrayList<Entry<String, Long>>(cgColdThresholdMapConfig.entrySet());
        configMapList.sort(new Comparator<Entry<String, Long>>() {
            @Override
            public int compare(Entry<String, Long> o1, Entry<String, Long> o2) {
                return (int)(o2.getValue() - o1.getValue());
            }
        });
        Iterator<Entry<String, Long>> iterator = configMapList.iterator();
        int maxDecelerate = 3;
        while (iterator.hasNext() && maxDecelerate > 0) {
            Entry<String, Long> next = iterator.next();
            if (!isAdminConfig(next.getKey())) {
                coldCtrStrategy.decelerate(next.getKey(), getThresholdByConsumerGroup(next.getKey()));
                maxDecelerate --;
            }
        }
    }

    public void coldAcc(String consumerGroup, long coldDataToAcc) {
        if (coldDataToAcc <= 0) {
            return;
        }
        GLOBAL_ACC.addAndGet(coldDataToAcc);
        AccAndTimeStamp atomicAcc = cgColdThresholdMapRuntime.get(consumerGroup);
        if (null == atomicAcc) {
            atomicAcc = new AccAndTimeStamp(new AtomicLong(coldDataToAcc));
            atomicAcc = cgColdThresholdMapRuntime.putIfAbsent(consumerGroup, atomicAcc);
        }
        if (null != atomicAcc) {
            atomicAcc.getColdAcc().addAndGet(coldDataToAcc);
            atomicAcc.setLastColdReadTimeMills(System.currentTimeMillis());
        }
    }

    public void addOrUpdateGroupConfig(String consumerGroup, Long threshold) {
        cgColdThresholdMapConfig.put(consumerGroup, threshold);
    }

    public void removeGroupConfig(String consumerGroup) {
        cgColdThresholdMapConfig.remove(consumerGroup);
    }

    /**【功能】判断参数给定的消费者组是不是需要进行"冷数据流量控制"*/
    public boolean isCgNeedColdDataFlowCtr(String consumerGroup) {
        //消息存储配置中是不是设置了冷数据流量控制
        if (!this.messageStoreConfig.isColdDataFlowControlEnable()) {
            return false;
        }
        //消费者组是不是系统组，系统组不走"冷数据流量控制"
        if (MixAll.isSysConsumerGroupForNoColdReadLimit(consumerGroup)) {
            return false;
        }
        //从cgColdThresholdMapRuntime获取消费者组的冷数据访问记录
        AccAndTimeStamp accAndTimeStamp = cgColdThresholdMapRuntime.get(consumerGroup);
        if (null == accAndTimeStamp) {
            return false;
        }
        //判断是不是超过了阈值
        Long threshold = getThresholdByConsumerGroup(consumerGroup);
        if (accAndTimeStamp.getColdAcc().get() >= threshold) {
            return true;
        }
        return GLOBAL_ACC.get() >= this.brokerConfig.getGlobalColdReadThreshold();
    }

    public boolean isGlobalColdCtr() {
        return GLOBAL_ACC.get() > this.brokerConfig.getGlobalColdReadThreshold();
    }

    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    /**【作用】读取给定消费者组的冷数据读取阈值。*/
    private Long getThresholdByConsumerGroup(String consumerGroup) {
        if (isAdminConfig(consumerGroup)) {
            if (consumerGroup.endsWith(ADAPTIVE)) {
                return cgColdThresholdMapConfig.get(consumerGroup.split(ADAPTIVE)[0]);
            }
            return cgColdThresholdMapConfig.get(consumerGroup);
        }
        Long threshold = null;
        if (brokerConfig.isColdCtrStrategyEnable()) {
            if (consumerGroup.endsWith(ADAPTIVE)) {
                threshold = cgColdThresholdMapConfig.get(consumerGroup);
            } else {
                threshold = cgColdThresholdMapConfig.get(buildAdaptiveKey(consumerGroup));
            }
        }
        if (null == threshold) {
            threshold = this.brokerConfig.getCgColdReadThreshold();
        }
        return threshold;
    }

    private String buildAdaptiveKey(String consumerGroup) {
        return consumerGroup + ADAPTIVE;
    }

    private boolean isAdminConfig(String consumerGroup) {
        if (consumerGroup.endsWith(ADAPTIVE)) {
            consumerGroup = consumerGroup.split(ADAPTIVE)[0];
        }
        return cgColdThresholdMapConfig.containsKey(consumerGroup);
    }

    private void clearAdaptiveConfig() {
        cgColdThresholdMapConfig.entrySet().removeIf(next -> next.getKey().endsWith(ADAPTIVE));
    }

}
