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

import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class RebalanceService extends ServiceThread {
    private static long waitInterval =
        Long.parseLong(System.getProperty(
            "rocketmq.client.rebalance.waitInterval", "20000"));
    private static long minInterval =
        Long.parseLong(System.getProperty(
            "rocketmq.client.rebalance.minInterval", "1000"));
    private final Logger log = LoggerFactory.getLogger(RebalanceService.class);
    private final MQClientInstance mqClientFactory;
    private long lastRebalanceTimestamp = System.currentTimeMillis();

    public RebalanceService(MQClientInstance mqClientFactory) {
        this.mqClientFactory = mqClientFactory;
    }

    /**【】：线程启动后就会执行run()。rocketmq这里服务的框架使用这种方式
     * 【功能】该服务默认是每间隔20秒*/
    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");
        /*realWaitInterval表示实际需要等待的时间，会进行动态调整*/
        long realWaitInterval = waitInterval;
        while (!this.isStopped()) {
            this.waitForRunning(realWaitInterval);
            /*lastRebalanceTimestamp是上一次再平衡的时间*/
            long interval = System.currentTimeMillis() - lastRebalanceTimestamp;
            if (interval < minInterval) { /*说明离上一次再平衡还不久即暂时不需要再平衡，仅更新realWaitInterval*/
                realWaitInterval = minInterval - interval;
            } else {
                /*doRebalance()执行负载均衡。
                如果均衡执行成功则更新realWaitInterval字段为waitInterval；否则更新为minInterval(以便在负载均衡失
                    败时更快的进行下一次的再平衡)*/
                boolean balanced = this.mqClientFactory.doRebalance();
                realWaitInterval = balanced ? waitInterval : minInterval;
                //从这里可以看出来lastRebalanceTimestamp时上一次再平衡时间(只要进行再平衡就会更新，并不是说再平衡成功了才更新)
                lastRebalanceTimestamp = System.currentTimeMillis();
            }
        }

        log.info(this.getServiceName() + " service end");
    }

    @Override
    public String getServiceName() {
        return RebalanceService.class.getSimpleName();
    }
}
