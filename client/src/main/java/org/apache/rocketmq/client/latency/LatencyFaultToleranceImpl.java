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

import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.common.ThreadLocalIndex;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**rocketmq提供的延迟容错机制的一种实现。主要目的是针对Broker状态的管理*/
public class LatencyFaultToleranceImpl implements LatencyFaultTolerance<String> {
    private final static Logger log = LoggerFactory.getLogger(MQFaultStrategy.class);
    /*BrokerName——>该broker集群的不可用时间*/
    private final ConcurrentHashMap<String, FaultItem> faultItemTable = new ConcurrentHashMap<String, FaultItem>(16);
    private int detectTimeout = 200;
    private int detectInterval = 2000;
    private final ThreadLocalIndex whichItemWorst = new ThreadLocalIndex();

    private volatile boolean startDetectorEnable = false;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "LatencyFaultToleranceScheduledThread");
        }
    });

    private final Resolver resolver;

    private final ServiceDetector serviceDetector;

    public LatencyFaultToleranceImpl(Resolver resolver, ServiceDetector serviceDetector) {
        this.resolver = resolver;
        this.serviceDetector = serviceDetector;
    }

    /**[]：对一组 Broker(就是faultItemTable中的条目)进行一轮可达性检测
     * 它通过定期检查每个Broker的状态，判断其是否可达，并更新其可用性标志*/
    public void detectByOneRound() {
        for (Map.Entry<String, FaultItem> item : this.faultItemTable.entrySet()) {
            FaultItem brokerItem = item.getValue();
            /*如果"当前时间"已经超过了"下次应该被检测时间(brokerItem.checkStamp属性指定)"，则表示这一
            个brokerItem需要检测一下，进入if块内部执行检测逻辑*/
            if (System.currentTimeMillis() - brokerItem.checkStamp >= 0) {
                //更新这个brokerItem下一次需要被检测的时间戳
                brokerItem.checkStamp = System.currentTimeMillis() + this.detectInterval;
                /*在DefaultMQProducerImpl的构造器中会创建LatencyFaultToleranceImpl，此时传递给构造
                器的resolver逻辑是：根据BrokerName返回id=0节点的brokerAddr*/
                String brokerAddr = resolver.resolve(brokerItem.getName());
                if (brokerAddr == null) {
                    faultItemTable.remove(item.getKey());
                    continue;
                }
                if (null == serviceDetector) {
                    continue;
                }
                /*调用 serviceDetector 的 detect 方法，检测指定地址（brokerAddr）的 Broker 是否可达。*/
                boolean serviceOK = serviceDetector.detect(brokerAddr, detectTimeout);
                //如果Broker可达，则更新brokerItem的可达性标志为true。。
                if (serviceOK && !brokerItem.reachableFlag) {
                    log.info(brokerItem.name + " is reachable now, then it can be used.");
                    brokerItem.reachableFlag = true;
                }
            }
        }
    }

    public void startDetector() {
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    if (startDetectorEnable) {
                        detectByOneRound();
                    }
                } catch (Exception e) {
                    log.warn("Unexpected exception raised while detecting service reachability", e);
                }
            }
        }, 3, 3, TimeUnit.SECONDS);
    }

    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }

    /**
     * []：用于更新name所指定的Broker集群故障项（FaultItem）
     * @param name：Broker集群名称
     * @param currentLatency :发送这个消息的延迟时间
     * @param notAvailableDuration : 根据两个数组计算出的 Broker不可用的持续时间
     * @param reachable : Broker的可达性
     * 实现逻辑：主要功能是根据 Broker 的延迟、不可用持续时间和可达性状态，动态
     *      更新 faultItemTable 中对应条目的信息。
     * */
    @Override
    public void updateFaultItem(final String name, final long currentLatency, final long notAvailableDuration,
                                final boolean reachable) {
        FaultItem old = this.faultItemTable.get(name);
        /*如果之前没有，则向faultItemTable添加一项*/
        if (null == old) {
            final FaultItem faultItem = new FaultItem(name);
            faultItem.setCurrentLatency(currentLatency);
            faultItem.updateNotAvailableDuration(notAvailableDuration);
            faultItem.setReachable(reachable);
            old = this.faultItemTable.putIfAbsent(name, faultItem);
        }
        /*如果之前存在brokerName这个条目，就需要更新该条目的属性*/
        if (null != old) {
            old.setCurrentLatency(currentLatency);
            old.updateNotAvailableDuration(notAvailableDuration);
            old.setReachable(reachable);
        }

        if (!reachable) {
            log.info(name + " is unreachable, it will not be used until it's reachable");
        }
    }

    /**【】：返回name这个broker集群是不是可用*/
    @Override
    public boolean isAvailable(final String name) {
        final FaultItem faultItem = this.faultItemTable.get(name);
        if (faultItem != null) {
            return faultItem.isAvailable();
        }
        return true;
    }

    /**疑问：和isAvailable的区别？？*/
    public boolean isReachable(final String name) {
        final FaultItem faultItem = this.faultItemTable.get(name);
        if (faultItem != null) {
            return faultItem.isReachable();
        }
        return true;
    }

    @Override
    public void remove(final String name) {
        this.faultItemTable.remove(name);
    }

    public boolean isStartDetectorEnable() {
        return startDetectorEnable;
    }

    public void setStartDetectorEnable(boolean startDetectorEnable) {
        this.startDetectorEnable = startDetectorEnable;
    }

    /**[]：随机返回一个可达(reachable)的BrokerName*/
    @Override
    public String pickOneAtLeast() {
        final Enumeration<FaultItem> elements = this.faultItemTable.elements();
        List<FaultItem> tmpList = new LinkedList<FaultItem>();
        while (elements.hasMoreElements()) {
            final FaultItem faultItem = elements.nextElement();
            tmpList.add(faultItem);
        }

        if (!tmpList.isEmpty()) {
            Collections.shuffle(tmpList);
            for (FaultItem faultItem : tmpList) {
                if (faultItem.reachableFlag) {
                    return faultItem.name;
                }
            }
        }

        return null;
    }

    @Override
    public String toString() {
        return "LatencyFaultToleranceImpl{" +
                "faultItemTable=" + faultItemTable +
                ", whichItemWorst=" + whichItemWorst +
                '}';
    }

    public void setDetectTimeout(final int detectTimeout) {
        this.detectTimeout = detectTimeout;
    }

    public void setDetectInterval(final int detectInterval) {
        this.detectInterval = detectInterval;
    }

    public class FaultItem implements Comparable<FaultItem> {
        private final String name;  //brokerName
        private volatile long currentLatency;  //最近一次的延迟时间
        private volatile long startTimestamp; //预测出来的可以使用的开始时间(会根据每一次延迟预测不可用时间)
        private volatile long checkStamp; //表示这个brokerName下一次需要被检查可达性的时间戳
        private volatile boolean reachableFlag; //表示当前的BrokerName(Broker集群)是否可达

        public FaultItem(final String name) {
            this.name = name;
        }

        /**
         * 【】：rocketmq会根据每次延迟预测不可用时间，这里就是根据预测的不可用时间更新这个broker集群从什么时候
         *      开始变得可用
         * 给startTimestamp赋值为:当前时间+computeNotAvailableDuration(isolation ? 10000 : currentLatency);的结
         *      果，这个startTimestamp参数在isAvailable()逻辑会用到
         * */
        public void updateNotAvailableDuration(long notAvailableDuration) {
            if (notAvailableDuration > 0 && System.currentTimeMillis() + notAvailableDuration > this.startTimestamp) {
                this.startTimestamp = System.currentTimeMillis() + notAvailableDuration;
                log.info(name + " will be isolated for " + notAvailableDuration + " ms.");
            }
        }

        @Override
        public int compareTo(final FaultItem other) {
            if (this.isAvailable() != other.isAvailable()) {
                if (this.isAvailable()) {
                    return -1;
                }

                if (other.isAvailable()) {
                    return 1;
                }
            }

            if (this.currentLatency < other.currentLatency) {
                return -1;
            } else if (this.currentLatency > other.currentLatency) {
                return 1;
            }

            if (this.startTimestamp < other.startTimestamp) {
                return -1;
            } else if (this.startTimestamp > other.startTimestamp) {
                return 1;
            }
            return 0;
        }

        public void setReachable(boolean reachableFlag) {
            this.reachableFlag = reachableFlag;
        }

        public void setCheckStamp(long checkStamp) {
            this.checkStamp = checkStamp;
        }

        public boolean isAvailable() {
            return System.currentTimeMillis() >= startTimestamp;
        }

        public boolean isReachable() {
            return reachableFlag;
        }

        @Override
        public int hashCode() {
            int result = getName() != null ? getName().hashCode() : 0;
            result = 31 * result + (int) (getCurrentLatency() ^ (getCurrentLatency() >>> 32));
            result = 31 * result + (int) (getStartTimestamp() ^ (getStartTimestamp() >>> 32));
            return result;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof FaultItem)) {
                return false;
            }

            final FaultItem faultItem = (FaultItem) o;

            if (getCurrentLatency() != faultItem.getCurrentLatency()) {
                return false;
            }
            if (getStartTimestamp() != faultItem.getStartTimestamp()) {
                return false;
            }
            return getName() != null ? getName().equals(faultItem.getName()) : faultItem.getName() == null;
        }

        @Override
        public String toString() {
            return "FaultItem{" +
                    "name='" + name + '\'' +
                    ", currentLatency=" + currentLatency +
                    ", startTimestamp=" + startTimestamp +
                    ", reachableFlag=" + reachableFlag +
                    '}';
        }

        public String getName() {
            return name;
        }

        public long getCurrentLatency() {
            return currentLatency;
        }

        public void setCurrentLatency(final long currentLatency) {
            this.currentLatency = currentLatency;
        }

        public long getStartTimestamp() {
            return startTimestamp;
        }

        public void setStartTimestamp(final long startTimestamp) {
            this.startTimestamp = startTimestamp;
        }

    }
}
