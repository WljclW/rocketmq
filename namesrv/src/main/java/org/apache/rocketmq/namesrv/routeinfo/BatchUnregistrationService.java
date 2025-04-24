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

package org.apache.rocketmq.namesrv.routeinfo;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.header.namesrv.UnRegisterBrokerRequestHeader;

/**
 * 提供了一种批处理Broker下线的机制，可以加速Broker的下线(offline)
 * BatchUnregistrationService provides a mechanism to unregister brokers in batch manner, which speeds up broker-offline
 * process.
 */
public class BatchUnregistrationService extends ServiceThread {
    private final RouteInfoManager routeInfoManager;
    private BlockingQueue<UnRegisterBrokerRequestHeader> unregistrationQueue;
    private static final Logger log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    public BatchUnregistrationService(RouteInfoManager routeInfoManager, NamesrvConfig namesrvConfig) {
        this.routeInfoManager = routeInfoManager;
        this.unregistrationQueue = new LinkedBlockingQueue<>(namesrvConfig.getUnRegisterBrokerQueueCapacity());
    }

    /**
     * Submits an unregister request to this queue.
     *
     * @param unRegisterRequest the request to submit
     * @return {@code true} if the request was added to this queue, else {@code false}
     */
    public boolean submit(UnRegisterBrokerRequestHeader unRegisterRequest) {
        return unregistrationQueue.offer(unRegisterRequest);
    }

    @Override
    public String getServiceName() {
        return BatchUnregistrationService.class.getName();
    }

    /**用于处理 Broker 注销请求的线程逻辑，通常运行在一个后台线程中。
     *    它的主要作用是从注销队列（unregistrationQueue，是一个阻塞队列）中取出多个注销请求，这些请求可能不止一个，最后调用
     * RouteInfoManager#unRegisterBroker方法完成Broker的下线。*/
    @Override
    public void run() {
        while (!this.isStopped()) {
            try {
                /*1. 一个值得思考的问题：为什么这里是先从unregistrationQueue拿出一个，然后将剩下的元素
                        提取到集合，再将拿出的那一个加进去？？为什么不是直接将所有的元素加进去？？
                    原因：take() 的保证————
                            在每次循环开始时，take() 确保至少有一个请求被取出并处理。如果没有元素的话，线程会阻塞到这里
                        drainTo 的不足————
                            如果只使用 drainTo，在队列为空的情况下，可能没有任何请求被处理，导致线程“空转”
                  2. drainTo()和take()方法的区别：
                     drainTo方法在队列为空时立即返回，并不会阻塞；一次可以获取阻塞队列的所有数据，一次加锁获取所有，效率高。
                     take()在队列为空的时候会阻塞等待直到有数据可以获取；一次只能获取队列中的一个元素，一次加锁只能获取一个。
                * */
                final UnRegisterBrokerRequestHeader request = unregistrationQueue.take();
                Set<UnRegisterBrokerRequestHeader> unregistrationRequests = new HashSet<>();
                unregistrationQueue.drainTo(unregistrationRequests);

                // Add polled request
                unregistrationRequests.add(request);

                this.routeInfoManager.unRegisterBroker(unregistrationRequests);
            } catch (Throwable e) {
                log.error("Handle unregister broker request failed", e);
            }
        }
    }

    // For test only
    int queueLength() {
        return this.unregistrationQueue.size();
    }
}
