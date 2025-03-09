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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeReturnType;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.remoting.protocol.body.CMResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**实现消息并发消费的核心服务类*/
public class ConsumeMessageConcurrentlyService implements ConsumeMessageService {
    private static final Logger log = LoggerFactory.getLogger(ConsumeMessageConcurrentlyService.class);
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl; //消费 推模式 实现
    private final DefaultMQPushConsumer defaultMQPushConsumer; //消费者 引用
    private final MessageListenerConcurrently messageListener; //并发消息监听事件回调
    private final BlockingQueue<Runnable> consumeRequestQueue; //消息消费任务队列
    private final ThreadPoolExecutor consumeExecutor; //消息消费线程池————消费消息
    private final String consumerGroup; //消息消费组
    /*scheduledExecutorService：添加消费任务到consumeExecutor队列的定时任务线程池(主要用于xxxLater方法，也就
    是说稍后多久才会真正干事)*/
    private final ScheduledExecutorService scheduledExecutorService;
    /*cleanExpireMsgExecutors:定时清理过期消息线程池*/
    private final ScheduledExecutorService cleanExpireMsgExecutors;

    public ConsumeMessageConcurrentlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
        MessageListenerConcurrently messageListener) {
        //初始化 defaultMQPushConsumerImpl、messageListener
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
        this.messageListener = messageListener;
        //本类字段 指向 外部的具体实现
        this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
        this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup(); //消费者组
        // 初始化"消费请求队列"为LinkedBlockingQueue无界队列
        this.consumeRequestQueue = new LinkedBlockingQueue<>();

        String consumerGroupTag = (consumerGroup.length() > 100 ? consumerGroup.substring(0, 100) : consumerGroup) + "_";
        // 初始化线程池，设置给字段 消费调度线程池
        this.consumeExecutor = new ThreadPoolExecutor(
            this.defaultMQPushConsumer.getConsumeThreadMin(),
            this.defaultMQPushConsumer.getConsumeThreadMax(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.consumeRequestQueue,
            new ThreadFactoryImpl("ConsumeMessageThread_" + consumerGroupTag));
        // 初始化"消费定时任务"线程池，线程数=1————注意实现xxxxLater方法(消费时出现异常时调用)
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_" + consumerGroupTag));
        // 初始化清除过期消息线程池，线程数=1
        this.cleanExpireMsgExecutors = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("CleanExpireMsgScheduledThread_" + consumerGroupTag));
    }

    /**start方法中就只是 开启了定期清理过期消息的服务。
     * consumeExecutor的使用是在有任务提交时使用；
     * scheduledExecutorService的使用是在该类的xxxxxLater方法，即有未来任务提交的时候使用*/
    public void start() {
        /*以固定的时间间隔 清理过期消息。。默认是每隔15min执行一次*/
        this.cleanExpireMsgExecutors.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    cleanExpireMsg();
                } catch (Throwable e) {
                    log.error("scheduleAtFixedRate cleanExpireMsg exception", e);
                }
            }

        }, this.defaultMQPushConsumer.getConsumeTimeout(), this.defaultMQPushConsumer.getConsumeTimeout(), TimeUnit.MINUTES);
    }

    /**关闭xxxxLater消费的线程、清理过期消息的线程、消费消息的线程池*/
    public void shutdown(long awaitTerminateMillis) {
        this.scheduledExecutorService.shutdown();
        ThreadUtils.shutdownGracefully(this.consumeExecutor, awaitTerminateMillis, TimeUnit.MILLISECONDS);
        this.cleanExpireMsgExecutors.shutdown();
    }

    @Override
    public void updateCorePoolSize(int corePoolSize) {
        if (corePoolSize > 0
            && corePoolSize <= Short.MAX_VALUE
            && corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax()) {
            this.consumeExecutor.setCorePoolSize(corePoolSize);
        }
    }

    @Override
    public void incCorePoolSize() {

    }

    @Override
    public void decCorePoolSize() {

    }

    @Override
    public int getCorePoolSize() {
        return this.consumeExecutor.getCorePoolSize();
    }

    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(MessageExt msg, String brokerName) {
        ConsumeMessageDirectlyResult result = new ConsumeMessageDirectlyResult();
        result.setOrder(false);
        result.setAutoCommit(true);

        msg.setBrokerName(brokerName);
        List<MessageExt> msgs = new ArrayList<>();
        msgs.add(msg);
        MessageQueue mq = new MessageQueue();
        mq.setBrokerName(brokerName);
        mq.setTopic(msg.getTopic());
        mq.setQueueId(msg.getQueueId());

        ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(mq);

        this.defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, this.consumerGroup);

        final long beginTime = System.currentTimeMillis();

        log.info("consumeMessageDirectly receive new message: {}", msg);

        try {
            ConsumeConcurrentlyStatus status = this.messageListener.consumeMessage(msgs, context);
            if (status != null) {
                switch (status) {
                    case CONSUME_SUCCESS:
                        result.setConsumeResult(CMResult.CR_SUCCESS);
                        break;
                    case RECONSUME_LATER:
                        result.setConsumeResult(CMResult.CR_LATER);
                        break;
                    default:
                        break;
                }
            } else {
                result.setConsumeResult(CMResult.CR_RETURN_NULL);
            }
        } catch (Throwable e) {
            result.setConsumeResult(CMResult.CR_THROW_EXCEPTION);
            result.setRemark(UtilAll.exceptionSimpleDesc(e));

            log.warn(String.format("consumeMessageDirectly exception: %s Group: %s Msgs: %s MQ: %s",
                UtilAll.exceptionSimpleDesc(e),
                ConsumeMessageConcurrentlyService.this.consumerGroup,
                msgs,
                mq), e);
        }

        result.setSpentTimeMills(System.currentTimeMillis() - beginTime);

        log.info("consumeMessageDirectly Result: {}", result);

        return result;
    }

    @Override
    public void submitConsumeRequest(
        final List<MessageExt> msgs, /*消息列表，默认每一次拉取最多32条消息*/
        final ProcessQueue processQueue, /*消息处理队列*/
        final MessageQueue messageQueue, /*消息所属的消息队列*/
        final boolean dispatchToConsume /*是否转发到消费者线程池，并发时忽略该参数*/
    ) {
        //获取批量消费数量
        final int consumeBatchSize = this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();
        /*下面的if-else的目的：根据消息的大小 对比 consumeBatchSize的大小 分情况处理*/
        /*
        * if块的逻辑：如果消息的大小 小于等于consumeBatchSize，组装消费请求，提交到消费线程池中进行消
        *       费操作。如果一场则稍后再次提交消费请求，通过方法submitConsumeRequestLater实现。
        * */
        if (msgs.size() <= consumeBatchSize) {
            ConsumeRequest consumeRequest = new ConsumeRequest(msgs, processQueue, messageQueue);
            try {
                this.consumeExecutor.submit(consumeRequest);
            } catch (RejectedExecutionException e) {
                this.submitConsumeRequestLater(consumeRequest);
            }
        } else {
            /*
            * 如果拉取的消息条数大于consumeBatchSize，则对拉取到消息进行分页处理；每页大小
            *       为：consumeBatchSize。通过循环迭代的方式，创建多个ConsumeRequest消费请
            *       求任务，提交到消费线程池中。如果触发拒绝提交异常，则稍后继续提交。实际上，
            *       由于任务队列是LinkedBlockingQueue无界队列，因此理论上不会出现拒绝提交。
            * */
            for (int total = 0; total < msgs.size(); ) {
                List<MessageExt> msgThis = new ArrayList<>(consumeBatchSize);
                for (int i = 0; i < consumeBatchSize; i++, total++) {
                    if (total < msgs.size()) {
                        msgThis.add(msgs.get(total));
                    } else {
                        break;
                    }
                }

                ConsumeRequest consumeRequest = new ConsumeRequest(msgThis, processQueue, messageQueue);
                try { /*提交消费任务到消费线程池*/
                    this.consumeExecutor.submit(consumeRequest);
                } catch (RejectedExecutionException e) { /*提交异常则稍后再提交*/
                    for (; total < msgs.size(); total++) {
                        msgThis.add(msgs.get(total));
                    }

                    this.submitConsumeRequestLater(consumeRequest);
                }
            }
        }
    }

    @Override
    public void submitPopConsumeRequest(final List<MessageExt> msgs,
        final PopProcessQueue processQueue,
        final MessageQueue messageQueue) {
        throw new UnsupportedOperationException();
    }

    /*定时对ProcessQueue进行处理，将其中的消息进行清理*/
    private void cleanExpireMsg() {
        Iterator<Map.Entry<MessageQueue, ProcessQueue>> it =
            this.defaultMQPushConsumerImpl.getRebalanceImpl().getProcessQueueTable().entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<MessageQueue, ProcessQueue> next = it.next();
            ProcessQueue pq = next.getValue();
            pq.cleanExpiredMsg(this.defaultMQPushConsumer);
        }
    }

    /**【总述】解析消费结果，主要是消费进度offset进行处理。*/
    public void processConsumeResult(
        final ConsumeConcurrentlyStatus status, // 并行消费结果
        final ConsumeConcurrentlyContext context, //并行消费上下文
        final ConsumeRequest consumeRequest //消费请求
    ) {
        int ackIndex = context.getAckIndex();

        if (consumeRequest.getMsgs().isEmpty())
            return;
        /*step1:判断消费结果————
        如果是CONSUMESUCCESS则设置ackIndex=msgs.size()-1;
        如果是RECONSUMELATER则设置ackIndex=-1。为发送消息确认ACK做准备。*/
        switch (status) {
            case CONSUME_SUCCESS:
                if (ackIndex >= consumeRequest.getMsgs().size()) {
                    ackIndex = consumeRequest.getMsgs().size() - 1;
                }
                int ok = ackIndex + 1;
                int failed = consumeRequest.getMsgs().size() - ok;
                this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), ok);
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), failed);
                break;
            case RECONSUME_LATER:
                ackIndex = -1;
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(),
                    consumeRequest.getMsgs().size());
                break;
            default:
                break;
        }
        /*step2:根据消费类型，进行处理————
        如果是广播模式：业务侧返回RECONSUME_LATER不会重新消费，只会打印告警日志；
        如果是集群模式，消息消费成功不执行sendMessageBack；当业务侧返回RECONSUME_LATER时，这
            批消息需要将ACK发送给broker。需要将它们重新封装为consumeRequest，延迟五秒后重新消费。*/
        switch (this.defaultMQPushConsumer.getMessageModel()) {
            case BROADCASTING:
                for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
                    MessageExt msg = consumeRequest.getMsgs().get(i);
                    log.warn("BROADCASTING, the message consume failed, drop it, {}", msg.toString());
                }
                break;
            case CLUSTERING:
                List<MessageExt> msgBackFailed = new ArrayList<>(consumeRequest.getMsgs().size());
                for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
                    MessageExt msg = consumeRequest.getMsgs().get(i);
                    // Maybe message is expired and cleaned, just ignore it.
                    if (!consumeRequest.getProcessQueue().containsMessage(msg)) {
                        log.info("Message is not found in its process queue; skip send-back-procedure, topic={}, "
                                + "brokerName={}, queueId={}, queueOffset={}", msg.getTopic(), msg.getBrokerName(),
                            msg.getQueueId(), msg.getQueueOffset());
                        continue;
                    }
                    boolean result = this.sendMessageBack(msg, context);
                    if (!result) {
                        msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                        msgBackFailed.add(msg);
                    }
                }

                if (!msgBackFailed.isEmpty()) {
                    consumeRequest.getMsgs().removeAll(msgBackFailed);

                    this.submitConsumeRequestLater(msgBackFailed, consumeRequest.getProcessQueue(), consumeRequest.getMessageQueue());
                }
                break;
            default:
                break;
        }
        /*step3:最后，从ProcessQueue中将这批成功消费的消息移除，通过offset更新消费进度；以便后续能够从上次的
        消费位点继续消费，避免重复消费。*/
        long offset = consumeRequest.getProcessQueue().removeMessage(consumeRequest.getMsgs());
        if (offset >= 0 && !consumeRequest.getProcessQueue().isDropped()) {
            this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(), offset, true);
        }
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
    }

    public boolean sendMessageBack(final MessageExt msg, final ConsumeConcurrentlyContext context) {
        int delayLevel = context.getDelayLevelWhenNextConsume();

        // Wrap topic with namespace before sending back message.
        msg.setTopic(this.defaultMQPushConsumer.withNamespace(msg.getTopic()));
        try {
            this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, this.defaultMQPushConsumer.queueWithNamespace(context.getMessageQueue()));
            return true;
        } catch (Exception e) {
            log.error("sendMessageBack exception, group: " + this.consumerGroup + " msg: " + msg, e);
        }

        return false;
    }

    /*方法中调用了submitConsumeRequest进行了消息消费处理。*/
    private void submitConsumeRequestLater(
        final List<MessageExt> msgs,
        final ProcessQueue processQueue,
        final MessageQueue messageQueue
    ) {

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessageConcurrentlyService.this.submitConsumeRequest(msgs, processQueue, messageQueue, true);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }

    /*通过scheduledExecutorService进行调度，每5秒再次提交一次消息消费请求。*/
    private void submitConsumeRequestLater(final ConsumeRequest consumeRequest
    ) {

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                //下面的就是消费消息的核心代码
                ConsumeMessageConcurrentlyService.this.consumeExecutor.submit(consumeRequest);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }

    class ConsumeRequest implements Runnable {
        private final List<MessageExt> msgs;
        private final ProcessQueue processQueue;
        private final MessageQueue messageQueue;

        public ConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue) {
            this.msgs = msgs;
            this.processQueue = processQueue;
            this.messageQueue = messageQueue;
        }

        public List<MessageExt> getMsgs() {
            return msgs;
        }

        public ProcessQueue getProcessQueue() {
            return processQueue;
        }

        /**
         * run方法中涉及到了：processQueue.dropped==true
         * 之所以当processQueue的dropped状态为true时不做任何处理，是因为当processQueue.dropped==true时，说
         * 明此时可能出现了新消费者的加入/原消费者down机等情况，导致原先消费者的队列在rebalance之后分配给了新
         * 的消费者。那么，这部分消息会被重新消费，因此此处就不需要做多余的处理，等待重新消费就可以了。
         * */
        @Override
        public void run() {
            /*step1:首先检查processQueue的dropped是否为true，如果是true，则停止消费，直接return。
            * 当发生消息rebalance时，会设置dropped==true，这么做的目的是防止消费者消费不属于
            * 自己的消息队列。*/
            if (this.processQueue.isDropped()) {
                log.info("the message queue not be able to consume, because it's dropped. group={} {}", ConsumeMessageConcurrentlyService.this.consumerGroup, this.messageQueue);
                return;
            }

            MessageListenerConcurrently listener = ConsumeMessageConcurrentlyService.this.messageListener;
            ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(messageQueue);
            ConsumeConcurrentlyStatus status = null;
            defaultMQPushConsumerImpl.tryResetPopRetryTopic(msgs, consumerGroup);
            defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, defaultMQPushConsumer.getConsumerGroup());
            /*step2:如果消费者存在钩子函数，则构建ConsumeMessageContext对象，执行executeHookBefore*/
            ConsumeMessageContext consumeMessageContext = null;
            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext = new ConsumeMessageContext();
                consumeMessageContext.setNamespace(defaultMQPushConsumer.getNamespace());
                consumeMessageContext.setConsumerGroup(defaultMQPushConsumer.getConsumerGroup());
                consumeMessageContext.setProps(new HashMap<>());
                consumeMessageContext.setMq(messageQueue);
                consumeMessageContext.setMsgList(msgs);
                consumeMessageContext.setSuccess(false);
                ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
            }
            /**step3是消费消息的核心逻辑*/
            /*step3:首先判断msgs是否为空，如果不为空，则迭代msgs，设置消费开始时间戳，回调客户端实现
            * 的MessageListenerConcurrently.consumeMessage方法执行具体消费逻辑，获得其消费结果status。*/
            long beginTimestamp = System.currentTimeMillis();
            boolean hasException = false;
            ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
            try {
                if (msgs != null && !msgs.isEmpty()) {
                    for (MessageExt msg : msgs) {
                        MessageAccessor.setConsumeStartTimeStamp(msg, String.valueOf(System.currentTimeMillis()));
                    }
                }
                // 通过Collections.unmodifiableList将msgs包装为不可修改的视图
                status = listener.consumeMessage(Collections.unmodifiableList(msgs), context);
            } catch (Throwable e) {
                log.warn(String.format("consumeMessage exception: %s Group: %s Msgs: %s MQ: %s",
                    UtilAll.exceptionSimpleDesc(e),
                    ConsumeMessageConcurrentlyService.this.consumerGroup,
                    msgs,
                    messageQueue), e);
                hasException = true; // 如果消费执行异常则hasException = true;
            }
            //计算 消费耗时
            long consumeRT = System.currentTimeMillis() - beginTimestamp;
            /*step4:根据具体的status返回值进行后续处理*/
            if (null == status) {  // 如果status为空
                if (hasException) {
                    returnType = ConsumeReturnType.EXCEPTION;
                } else {
                    returnType = ConsumeReturnType.RETURNNULL;
                }
            } else if (consumeRT >= defaultMQPushConsumer.getConsumeTimeout() * 60 * 1000) { // 消费超时
                returnType = ConsumeReturnType.TIME_OUT;
            } else if (ConsumeConcurrentlyStatus.RECONSUME_LATER == status) { // 业务侧返回RECONSUME_LATER，需要重新消费，returnType为消费失败
                returnType = ConsumeReturnType.FAILED;
            } else if (ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status) {  // 业务侧返回CONSUME_SUCCESS，消费成功，returnType为消费成功
                returnType = ConsumeReturnType.SUCCESS;
            }

            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
            }
            // 如果客户端返回的status为null，则赋值为RECONSUME_LATER，以便重复消费
            if (null == status) {
                log.warn("consumeMessage return null, Group: {} Msgs: {} MQ: {}",
                    ConsumeMessageConcurrentlyService.this.consumerGroup,
                    msgs,
                    messageQueue);
                status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
            /*step5:如果存在钩子函数，则执行钩子函数executeHookAfter*/
            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext.setStatus(status.toString());
                consumeMessageContext.setSuccess(ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status);
                consumeMessageContext.setAccessChannel(defaultMQPushConsumer.getAccessChannel());
                ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
            }

            ConsumeMessageConcurrentlyService.this.getConsumerStatsManager()
                .incConsumeRT(ConsumeMessageConcurrentlyService.this.consumerGroup, messageQueue.getTopic(), consumeRT);
            /* step7:执行消费逻辑之后，再次判断processQueue的dropped状态；如果为true，则不进行任
            * 何处理；当非true时，调用processConsumeResult对消费结果进行处理。*/
            if (!processQueue.isDropped()) {
                ConsumeMessageConcurrentlyService.this.processConsumeResult(status, context, this);
            } else {
                log.warn("processQueue is dropped without process consume result. messageQueue={}, msgs={}", messageQueue, msgs);
            }
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

    }
}
