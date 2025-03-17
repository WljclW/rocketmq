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

package org.apache.rocketmq.common.stats;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.org.slf4j.Logger;

/**【总述】：一类统计指标集合，其内部主要维护的数据结构为ConcurrentMap<String, StatsItem>statsItemTable。
        以指标TOPIC_PUT_NUMS对应的StatsItemSet为例，StatsItemSet存储的是主题写入数量（消息发送
        数量），内部维护的statsItemTable的key为主题的名称，StatsItem为该主题对应的统计信息
 【说明】
    1. 方法类似，可以看addValue方法的注释来启发其他方法的理解*/
public class StatsItemSet {
    private final ConcurrentMap<String/* key */, StatsItem> statsItemTable =
        new ConcurrentHashMap<>(128);

    private final String statsName;
    private final ScheduledExecutorService scheduledExecutorService;

    private final Logger logger;

    public StatsItemSet(String statsName, ScheduledExecutorService scheduledExecutorService, Logger logger) {
        this.logger = logger;
        this.statsName = statsName;
        this.scheduledExecutorService = scheduledExecutorService;
        this.init();
    }

    /**[]:启动6个定时任务
     * rocketmq实现TPS计算的出处：就包含在StatsItemSet中。在构造init()方法的时候定义
     * 了6个定时任务，用来对监控数据进行采样计算
     * */
    public void init() {
        /*分钟级别的采样，每10秒执行一次samplingInSeconds()*/
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    samplingInSeconds();
                } catch (Throwable ignored) {
                }
            }
        }, 0, 10, TimeUnit.SECONDS);
        /*小时级别的采样，每10分钟执行一次samplingInMinutes()*/
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    samplingInMinutes();
                } catch (Throwable ignored) {
                }
            }
        }, 0, 10, TimeUnit.MINUTES);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    samplingInHour();
                } catch (Throwable ignored) {
                }
            }
        }, 0, 1, TimeUnit.HOURS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    printAtMinutes();
                } catch (Throwable ignored) {
                }
            }
        }, Math.abs(UtilAll.computeNextMinutesTimeMillis() - System.currentTimeMillis()), 1000 * 60, TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    printAtHour();
                } catch (Throwable ignored) {
                }
            }
        }, Math.abs(UtilAll.computeNextHourTimeMillis() - System.currentTimeMillis()), 1000 * 60 * 60, TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    printAtDay();
                } catch (Throwable ignored) {
                }
            }
        }, Math.abs(UtilAll.computeNextMorningTimeMillis() - System.currentTimeMillis()), 1000 * 60 * 60 * 24, TimeUnit.MILLISECONDS);
    }

    private void samplingInSeconds() {
        Iterator<Entry<String, StatsItem>> it = this.statsItemTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, StatsItem> next = it.next();
            next.getValue().samplingInSeconds();
        }
    }

    private void samplingInMinutes() {
        Iterator<Entry<String, StatsItem>> it = this.statsItemTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, StatsItem> next = it.next();
            next.getValue().samplingInMinutes();
        }
    }

    private void samplingInHour() {
        Iterator<Entry<String, StatsItem>> it = this.statsItemTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, StatsItem> next = it.next();
            next.getValue().samplingInHour();
        }
    }

    private void printAtMinutes() {
        Iterator<Entry<String, StatsItem>> it = this.statsItemTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, StatsItem> next = it.next();
            next.getValue().printAtMinutes();
        }
    }

    private void printAtHour() {
        Iterator<Entry<String, StatsItem>> it = this.statsItemTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, StatsItem> next = it.next();
            next.getValue().printAtHour();
        }
    }

    private void printAtDay() {
        Iterator<Entry<String, StatsItem>> it = this.statsItemTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, StatsItem> next = it.next();
            next.getValue().printAtDay();
        }
    }

    /**[]:
     * 详细思路：根据统计key，例如topic的名称，从ConcurrentMap<String,StatsItem>statsItemTable中获
     * 取键statsKey对应的统计信息。比如：
     * BrokerStatsManager#incTopicPutNums方法中会用到addValue方法，此时：statsKey对应的就是topic。
     * topic的写入总数用StatsItem表示，如果statsItemTable中未包含该topic对应的StatsItem，则创建一个
     * 新的对象，然后通过原子的方式新增Value与Times这两个属性的值。
     * 经过上述的步骤就完成了topic写入数量的统计*/
    public void addValue(final String statsKey, final int incValue, final int incTimes) {
        StatsItem statsItem = this.getAndCreateStatsItem(statsKey);
        statsItem.getValue().add(incValue);
        statsItem.getTimes().add(incTimes);
    }

    public void addRTValue(final String statsKey, final int incValue, final int incTimes) {
        StatsItem statsItem = this.getAndCreateRTStatsItem(statsKey);
        statsItem.getValue().add(incValue);
        statsItem.getTimes().add(incTimes);
    }

    public void delValue(final String statsKey) {
        StatsItem statsItem = this.statsItemTable.get(statsKey);
        if (null != statsItem) {
            this.statsItemTable.remove(statsKey);
        }
    }

    public void delValueByPrefixKey(final String statsKey, String separator) {
        Iterator<Entry<String, StatsItem>> it = this.statsItemTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, StatsItem> next = it.next();
            if (next.getKey().startsWith(statsKey + separator)) {
                it.remove();
            }
        }
    }

    public void delValueByInfixKey(final String statsKey, String separator) {
        Iterator<Entry<String, StatsItem>> it = this.statsItemTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, StatsItem> next = it.next();
            if (next.getKey().contains(separator + statsKey + separator)) {
                it.remove();
            }
        }
    }

    public void delValueBySuffixKey(final String statsKey, String separator) {
        Iterator<Entry<String, StatsItem>> it = this.statsItemTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, StatsItem> next = it.next();
            if (next.getKey().endsWith(separator + statsKey)) {
                it.remove();
            }
        }
    }

    public StatsItem getAndCreateStatsItem(final String statsKey) {
        return getAndCreateItem(statsKey, false);
    }

    public StatsItem getAndCreateRTStatsItem(final String statsKey) {
        return getAndCreateItem(statsKey, true);
    }

    public StatsItem getAndCreateItem(final String statsKey, boolean rtItem) {
        StatsItem statsItem = this.statsItemTable.get(statsKey);
        if (null == statsItem) {
            if (rtItem) {
                statsItem = new RTStatsItem(this.statsName, statsKey, this.scheduledExecutorService, logger);
            } else {
                statsItem = new StatsItem(this.statsName, statsKey, this.scheduledExecutorService, logger);
            }
            StatsItem prev = this.statsItemTable.putIfAbsent(statsKey, statsItem);

            if (null != prev) {
                statsItem = prev;
                // statsItem.init();
            }
        }

        return statsItem;
    }

    public StatsSnapshot getStatsDataInMinute(final String statsKey) {
        StatsItem statsItem = this.statsItemTable.get(statsKey);
        if (null != statsItem) {
            return statsItem.getStatsDataInMinute();
        }
        return new StatsSnapshot();
    }

    public StatsSnapshot getStatsDataInHour(final String statsKey) {
        StatsItem statsItem = this.statsItemTable.get(statsKey);
        if (null != statsItem) {
            return statsItem.getStatsDataInHour();
        }
        return new StatsSnapshot();
    }

    public StatsSnapshot getStatsDataInDay(final String statsKey) {
        StatsItem statsItem = this.statsItemTable.get(statsKey);
        if (null != statsItem) {
            return statsItem.getStatsDataInDay();
        }
        return new StatsSnapshot();
    }

    public StatsItem getStatsItem(final String statsKey) {
        return this.statsItemTable.get(statsKey);
    }
}
