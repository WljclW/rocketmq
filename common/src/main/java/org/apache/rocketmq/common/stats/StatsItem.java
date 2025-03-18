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

import java.util.LinkedList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.org.slf4j.Logger;

/**具体统计数据的载体*/
public class StatsItem {
    //当前统计的数据值
    private final LongAdder value = new LongAdder();
    //value值变化的次数
    private final LongAdder times = new LongAdder();
    /*近1min内的调用快照信息，每10s采集一次，并且超过6个则淘汰最早入队的原生快照
    信息，故其长度不会超过6*/
    private final LinkedList<CallSnapshot> csListMinute = new LinkedList<>();
    /*近1h内的调用快照信息，每10min采集一次，同样不会超过6个元素*/
    private final LinkedList<CallSnapshot> csListHour = new LinkedList<>();
    /*近一天的调用快照信息，每1h采集一次，该队列长度不会超过24，超过则会丢弃最早
    入队的*/
    private final LinkedList<CallSnapshot> csListDay = new LinkedList<>();
    /*统计项的名称，与StatsItemSet中的statsName相同*/
    private final String statsName;
    /*统计项Key。如果statsName统计各topic的写入数量，则statsKey为每一个具体的topic名称.
    * 简单理解：就是指统计项是针对谁的*/
    private final String statsKey;
    private final ScheduledExecutorService scheduledExecutorService;

    private final Logger logger;

    public StatsItem(String statsName, String statsKey, ScheduledExecutorService scheduledExecutorService, Logger logger) {
        this.statsName = statsName;
        this.statsKey = statsKey;
        this.scheduledExecutorService = scheduledExecutorService;
        this.logger = logger;
    }

    /**
     * @Description : 根据采样得到的数据计算统计指标。举个例子：
     *  在1min内，消息发送者使用了RocketMQ的批量消息，一次发送10条，共调用批量接口发送了6次消息，那上面各个值是怎么计算的呢？
     *      1）sum：60。
     *      2）tps：60/60(s) = 1tps，用sum除以时间即可。
     *      3）avgpt：用sum除以count，最终计算结果为10，表示一次调用改变的平均数值。
     * @param csList:一个快照的list(采样存放的容器)。
     * */
    private static StatsSnapshot computeStatsData(final LinkedList<CallSnapshot> csList) {
        StatsSnapshot statsSnapshot = new StatsSnapshot();
        synchronized (csList) {
            double tps = 0;
            //表示单位时间内从一个快照值到另外一个快照值发生变化的速率
            double avgpt = 0;
            long sum = 0;
            long timesDiff = 0;
            if (!csList.isEmpty()) {
                CallSnapshot first = csList.getFirst();
                CallSnapshot last = csList.getLast();
                /*sum的计算逻辑”最后一个统计量-第一个统计量“*/
                sum = last.getValue() - first.getValue();
                /*tps的计算逻辑“sum/时间差”。单位需要换算成秒，因此分子乘了1000*/
                tps = (sum * 1000.0d) / (last.getTimestamp() - first.getTimestamp());

                timesDiff = last.getTimes() - first.getTimes();
                if (timesDiff > 0) {
                    avgpt = (sum * 1.0d) / timesDiff;
                }
            }

            statsSnapshot.setSum(sum);
            statsSnapshot.setTps(tps);
            statsSnapshot.setAvgpt(avgpt);
            statsSnapshot.setTimes(timesDiff);
        }

        return statsSnapshot;
    }

    public StatsSnapshot getStatsDataInMinute() {
        return computeStatsData(this.csListMinute);
    }

    public StatsSnapshot getStatsDataInHour() {
        return computeStatsData(this.csListHour);
    }

    public StatsSnapshot getStatsDataInDay() {
        return computeStatsData(this.csListDay);
    }

    public void init() {

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    samplingInSeconds();
                } catch (Throwable ignored) {
                }
            }
        }, 0, 10, TimeUnit.SECONDS);

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
        }, Math.abs(UtilAll.computeNextMorningTimeMillis() - System.currentTimeMillis()) - 2000, 1000 * 60 * 60 * 24, TimeUnit.MILLISECONDS);
    }

    /**【】：samplingInSeconds、samplingInMinutes、samplingInHours这几个方法都是每隔一段时间生
     *      成一个快照，放入到LinkedList<CallSnapshot>类型的字段中
     * 方法的执行逻辑：
     * 根据当前的时间戳、变更次数、调用次数创建一个快照，将其存入csListMinute变量，如果该容器中的元素
     *  超过7个，则将其头部元素移除，即确保csListMinute最多存储7个元素因此整体的思路就是：在分钟级采
     *  样容器中存储最近1min的采样数据，每隔10s采集1次快照。这7个元素中，第一个元素是一分钟前记录的数
     *  据，后面的6个是最近一分钟内的统计数据
     * 下面的samplingInMinutes方法执行逻辑基本相同，只是时间间隔不同;samplingInHour方法执行逻辑
     *  也一样
     * 如何计算tps：
     *      用两个快照数据的差值 除以 两个快照之间的时间间隔*/
    public void samplingInSeconds() {
        synchronized (this.csListMinute) {
            if (this.csListMinute.size() == 0) {
                this.csListMinute.add(new CallSnapshot(System.currentTimeMillis() - 10 * 1000, 0, 0));
            }
            this.csListMinute.add(new CallSnapshot(System.currentTimeMillis(), this.times.sum(), this.value
                .sum()));
            if (this.csListMinute.size() > 7) {
                this.csListMinute.removeFirst();
            }
        }
    }

    public void samplingInMinutes() {
        synchronized (this.csListHour) {
            if (this.csListHour.size() == 0) {
                this.csListHour.add(new CallSnapshot(System.currentTimeMillis() - 10 * 60 * 1000, 0, 0));
            }
            this.csListHour.add(new CallSnapshot(System.currentTimeMillis(), this.times.sum(), this.value
                .sum()));
            if (this.csListHour.size() > 7) {
                this.csListHour.removeFirst();
            }
        }
    }

    public void samplingInHour() {
        synchronized (this.csListDay) {
            if (this.csListDay.size() == 0) {
                this.csListDay.add(new CallSnapshot(System.currentTimeMillis() - 1 * 60 * 60 * 1000, 0, 0));
            }
            this.csListDay.add(new CallSnapshot(System.currentTimeMillis(), this.times.sum(), this.value
                .sum()));
            if (this.csListDay.size() > 25) {
                this.csListDay.removeFirst();
            }
        }
    }

    /**[]：该方法每分钟执行1次，将计算出来的监控指标(计算指标的逻辑是在computeStatsData方法)以日志文件的形式输出
     *  在RocketMQ的日志文件中，其路径默认为${user.home}/logs/rocketmqlogs/stats.log。*/
    public void printAtMinutes() {
        StatsSnapshot ss = computeStatsData(this.csListMinute);
        logger.info(String.format("[%s] [%s] Stats In One Minute, ", this.statsName, this.statsKey) + statPrintDetail(ss));
    }

    public void printAtHour() {
        StatsSnapshot ss = computeStatsData(this.csListHour);
        logger.info(String.format("[%s] [%s] Stats In One Hour, ", this.statsName, this.statsKey) + statPrintDetail(ss));

    }

    public void printAtDay() {
        StatsSnapshot ss = computeStatsData(this.csListDay);
        logger.info(String.format("[%s] [%s] Stats In One Day, ", this.statsName, this.statsKey) + statPrintDetail(ss));
    }

    protected String statPrintDetail(StatsSnapshot ss) {
        return String.format("SUM: %d TPS: %.2f AVGPT: %.2f",
                ss.getSum(),
                ss.getTps(),
                ss.getAvgpt());
    }

    public LongAdder getValue() {
        return value;
    }

    public String getStatsKey() {
        return statsKey;
    }

    public String getStatsName() {
        return statsName;
    }

    public LongAdder getTimes() {
        return times;
    }
}

/**统计快照*/
class CallSnapshot {
    private final long timestamp; //生成快照时的时间戳
    private final long times; //快照生成时value值变化的次数
    private final long value; //快照生成时 统计量的值

    public CallSnapshot(long timestamp, long times, long value) {
        super();
        this.timestamp = timestamp;
        this.times = times;
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getTimes() {
        return times;
    }

    public long getValue() {
        return value;
    }
}
