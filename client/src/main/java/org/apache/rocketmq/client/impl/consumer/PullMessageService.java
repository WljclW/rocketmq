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

import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.message.MessageRequestMode;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * 【总述】消息拉取服务线程，从Broker拉取消息供消费者消费。。
 * 1. 基本上所有的方法都会在DefaultMQPushConsumerImpl类中被使用
 * 2. run方法是真正"执行拉取任务的方法"，根据拿取消息的模式会调用DefaultMQPushConsumerImpl的对应方法(popMessage或者pullMessage)*/
public class PullMessageService extends ServiceThread {
    private final Logger logger = LoggerFactory.getLogger(PullMessageService.class);
    private final LinkedBlockingQueue<MessageRequest> messageRequestQueue = new LinkedBlockingQueue<>();

    private final MQClientInstance mQClientFactory;
    /**创建单一的线程池，指定线程工厂。。。rocketmq中通过这种方式给创建的线程增加前缀，方便区分业务线程*/
    private final ScheduledExecutorService scheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactoryImpl("PullMessageServiceScheduledThread"));

    public PullMessageService(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    /**将一个PullRequest延迟timeDelay放入到messageRequestQueue*/
    public void executePullRequestLater(final PullRequest pullRequest, final long timeDelay) {
        if (!isStopped()) {
            /**schedule创建一个 再指定延迟后执行 的一次性任务*/
            this.scheduledExecutorService.schedule(new Runnable() {
                @Override
                public void run() {
                    PullMessageService.this.executePullRequestImmediately(pullRequest); /*延迟拉取的实现原理：过一段时间(定时任务)执行立即拉取方法(xxxxImmediately)*/
                }
            }, timeDelay, TimeUnit.MILLISECONDS);
        } else {
            logger.warn("PullMessageServiceScheduledThread has shutdown");
        }
    }

    /*将一个PullRequest立即放入到messageRequestQueue。。
    【注意】立即拉取并不是实时的，只是将PullRequest放入在阻塞队列。。在PullMessageService的run方法中会不断从阻塞队列take请
            求，然后向broker拉消息*/
    public void executePullRequestImmediately(final PullRequest pullRequest) {
        try {
            this.messageRequestQueue.put(pullRequest);
        } catch (InterruptedException e) {
            logger.error("executePullRequestImmediately pullRequestQueue.put", e);
        }
    }

    /*executePopPullRequestLater和executePopPullRequestImmediately与上面的两个方法是类似的..
    * 区别仅仅在于：往阻塞队列中放入的是PopRequest，而不再是PullRequest*/
    public void executePopPullRequestLater(final PopRequest popRequest, final long timeDelay) {
        if (!isStopped()) {
            this.scheduledExecutorService.schedule(new Runnable() {
                @Override
                public void run() {
                    PullMessageService.this.executePopPullRequestImmediately(popRequest);
                }
            }, timeDelay, TimeUnit.MILLISECONDS);
        } else {
            logger.warn("PullMessageServiceScheduledThread has shutdown");
        }
    }

    public void executePopPullRequestImmediately(final PopRequest popRequest) {
        try {
            this.messageRequestQueue.put(popRequest);
        } catch (InterruptedException e) {
            logger.error("executePullRequestImmediately pullRequestQueue.put", e);
        }
    }

    public void executeTaskLater(final Runnable r, final long timeDelay) {
        if (!isStopped()) {
            this.scheduledExecutorService.schedule(r, timeDelay, TimeUnit.MILLISECONDS);
        } else {
            logger.warn("PullMessageServiceScheduledThread has shutdown");
        }
    }

    public void executeTask(final Runnable r) {
        if (!isStopped()) {
            this.scheduledExecutorService.execute(r);
        } else {
            logger.warn("PullMessageServiceScheduledThread has shutdown");
        }
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    /**
     * pullMessage/popMessage方法都是从阻塞队列中取出拿消息的请求；然后根据请求中的消费者组从consmerTbale中取出MQConsumerInner
     *      如果找不到会报错，如果找到了就会根据pullMessage方法
     * */
    private void pullMessage(final PullRequest pullRequest) {
        final MQConsumerInner consumer = this.mQClientFactory.selectConsumer(pullRequest.getConsumerGroup());
        if (consumer != null) {
            DefaultMQPushConsumerImpl impl = (DefaultMQPushConsumerImpl) consumer;
            impl.pullMessage(pullRequest);
        } else {
            logger.warn("No matched consumer for the PullRequest {}, drop it", pullRequest);
        }
    }

    private void popMessage(final PopRequest popRequest) {  //见pullMessage方法注释
        final MQConsumerInner consumer = this.mQClientFactory.selectConsumer(popRequest.getConsumerGroup());
        if (consumer != null) {
            DefaultMQPushConsumerImpl impl = (DefaultMQPushConsumerImpl) consumer;
            impl.popMessage(popRequest);
        } else {
            logger.warn("No matched consumer for the PopRequest {}, drop it", popRequest);
        }
    }

    /**
     * 亮点：从阻塞队列获取的时候使用MessageRequest(体现面向接口编程)、从阻塞队列获取任务的take()
     * */
    @Override
    public void run() {
        logger.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                /**从messageRequestQueue中获取一个messageRequest,根据MessageRequestMode来选取不同
                 * 的方法请求消息..阻塞队列在获取的时候，如果是空的，进行等待*/
                MessageRequest messageRequest = this.messageRequestQueue.take();
                if (messageRequest.getMessageRequestMode() == MessageRequestMode.POP) {
                    this.popMessage((PopRequest) messageRequest);
                } else {
                    this.pullMessage((PullRequest) messageRequest);
                }
            } catch (InterruptedException ignored) {
            } catch (Exception e) {
                logger.error("Pull Message Service Run Method exception", e);
            }
        }

        logger.info(this.getServiceName() + " service end");
    }

    @Override
    public void shutdown(boolean interrupt) {
        super.shutdown(interrupt);
        ThreadUtils.shutdownGracefully(this.scheduledExecutorService, 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public String getServiceName() {
        return PullMessageService.class.getSimpleName();
    }

}
