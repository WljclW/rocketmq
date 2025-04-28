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
package org.apache.rocketmq.store.queue;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.QueueTypeUtils;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.SelectMappedBufferResult;

import static java.lang.String.format;
import static org.apache.rocketmq.store.config.StorePathConfigHelper.getStorePathBatchConsumeQueue;
import static org.apache.rocketmq.store.config.StorePathConfigHelper.getStorePathConsumeQueue;

public class ConsumeQueueStore extends AbstractConsumeQueueStore {

    public ConsumeQueueStore(DefaultMessageStore messageStore) {
        super(messageStore);
    }

    @Override
    public void start() {
        log.info("Default ConsumeQueueStore start!");
    }

    @Override
    public boolean load() {
        boolean cqLoadResult = loadConsumeQueues(getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()), CQType.SimpleCQ);
        boolean bcqLoadResult = loadConsumeQueues(getStorePathBatchConsumeQueue(this.messageStoreConfig.getStorePathRootDir()), CQType.BatchCQ);
        return cqLoadResult && bcqLoadResult;
    }

    @Override
    public boolean loadAfterDestroy() {
        return true;
    }

    /**依次拿到每一个consumequeue进行恢复*/
    @Override
    public void recover() {
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueueInterface logic : maps.values()) {
                this.recover(logic);
            }
        }
    }

    @Override
    public boolean recoverConcurrently() {
        int count = 0;
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
            count += maps.values().size();
        }
        final CountDownLatch countDownLatch = new CountDownLatch(count);
        BlockingQueue<Runnable> recoverQueue = new LinkedBlockingQueue<>();
        final ExecutorService executor = buildExecutorService(recoverQueue, "RecoverConsumeQueueThread_");
        List<FutureTask<Boolean>> result = new ArrayList<>(count);
        try {
            for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
                for (final ConsumeQueueInterface logic : maps.values()) {
                    FutureTask<Boolean> futureTask = new FutureTask<>(() -> {
                        boolean ret = true;
                        try {
                            logic.recover();
                        } catch (Throwable e) {
                            ret = false;
                            log.error("Exception occurs while recover consume queue concurrently, " +
                                "topic={}, queueId={}", logic.getTopic(), logic.getQueueId(), e);
                        } finally {
                            countDownLatch.countDown();
                        }
                        return ret;
                    });

                    result.add(futureTask);
                    executor.submit(futureTask);
                }
            }
            countDownLatch.await();
            for (FutureTask<Boolean> task : result) {
                if (task != null && task.isDone()) {
                    if (!task.get()) {
                        return false;
                    }
                }
            }
        } catch (Exception e) {
            log.error("Exception occurs while recover consume queue concurrently", e);
            return false;
        } finally {
            executor.shutdown();
        }
        return true;
    }

    @Override
    public boolean shutdown() {
        return true;
    }

    @Override
    public long rollNextFile(ConsumeQueueInterface consumeQueue, final long offset) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.rollNextFile(offset);
    }

    public void correctMinOffset(ConsumeQueueInterface consumeQueue, long minCommitLogOffset) {
        consumeQueue.correctMinOffset(minCommitLogOffset);
    }

    @Override
    public void putMessagePositionInfoWrapper(DispatchRequest dispatchRequest) {
        //根据主题 和 队列ID，获取对应的ConsumeQueue实例
        ConsumeQueueInterface cq = this.findOrCreateConsumeQueue(dispatchRequest.getTopic(), dispatchRequest.getQueueId());
        //存储逻辑的实现
        this.putMessagePositionInfoWrapper(cq, dispatchRequest);
    }

    @Override
    public List<ByteBuffer> rangeQuery(String topic, int queueId, long startIndex, int num) {
        return null;
    }

    @Override
    public ByteBuffer get(String topic, int queueId, long startIndex) {
        return null;
    }

    @Override
    public long getMaxOffsetInQueue(String topic, int queueId) {
        ConsumeQueueInterface logic = findOrCreateConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getMaxOffsetInQueue();
        }
        return 0;
    }

    @Override
    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp, BoundaryType boundaryType) {
        ConsumeQueueInterface logic = findOrCreateConsumeQueue(topic, queueId);
        if (logic != null) {
            long resultOffset = logic.getOffsetInQueueByTime(timestamp, boundaryType);
            // Make sure the result offset is in valid range.
            resultOffset = Math.max(resultOffset, logic.getMinOffsetInQueue());
            resultOffset = Math.min(resultOffset, logic.getMaxOffsetInQueue());
            return resultOffset;
        }
        return 0;
    }

    private FileQueueLifeCycle getLifeCycle(String topic, int queueId) {
        return findOrCreateConsumeQueue(topic, queueId);
    }

    public boolean load(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.load();
    }

    /**
     * 【功能】完成所有的ConsumeQueue的加载(会创建ConsumeQueue实例) 并记录。
     *          比如：storePath可能是“D:\IDEA_projects\STORE_DATA\ROCKETMQ_DATA\consumequeue”
     * 【实现流程】首先拿到consumequeue文件夹路径，遍历里面所有的文件夹或者文件组成fileTopicList（这个列表里其实就是所有topic文件夹的绝对
     * 路径）；遍历fileTopicList中所有的名字，针对每一个元素再遍历该文件夹下的所有名字得到fileQueueIdList（这个文件夹其实就是某一个topic
     * 下所有队列log文件夹的绝对路径，结尾分别是0，1，2...表示队列id）
     * */
    private boolean loadConsumeQueues(String storePath/*consumequeue文件夹路径*/, CQType cqType) {
        File dirLogic = new File(storePath);
        File[] fileTopicList = dirLogic.listFiles(); //列出某一个路径下所有的“文件”以及“文件夹”名称组成的列表
        if (fileTopicList != null) {

            for (File fileTopic : fileTopicList) {
                String topic = fileTopic.getName(); //返回列表中的名称。即拿到fileTopic最后一个分隔符后面的子串

                File[] fileQueueIdList = fileTopic.listFiles();
                if (fileQueueIdList != null) { //fileQueueIdList是某一个topic所有队列问价夹的绝对路径
                    for (File fileQueueId : fileQueueIdList) { //fileQueueId就是某一个topic的某一个队列id
                        int queueId;
                        try {
                            queueId = Integer.parseInt(fileQueueId.getName());
                        } catch (NumberFormatException e) {
                            continue;
                        }

                        queueTypeShouldBe(topic, cqType);
                        /*根据消息队列路径完成文件的加载到内存*/
                        ConsumeQueueInterface logic = createConsumeQueueByType(cqType, topic, queueId, storePath);
                        this.putConsumeQueue(topic, queueId, logic);
                        if (!this.load(logic)) {
                            return false;
                        }
                    }
                }
            }
        }

        log.info("load {} all over, OK", cqType);

        return true;
    }

    private ConsumeQueueInterface createConsumeQueueByType(CQType cqType, String topic, int queueId, String storePath) {
        if (Objects.equals(CQType.SimpleCQ, cqType)) {
            return new ConsumeQueue(
                topic,
                queueId,
                storePath,
                this.messageStoreConfig.getMappedFileSizeConsumeQueue(),
                this.messageStore);
        } else if (Objects.equals(CQType.BatchCQ, cqType)) {
            return new BatchConsumeQueue(
                topic,
                queueId,
                storePath,
                this.messageStoreConfig.getMapperFileSizeBatchConsumeQueue(),
                this.messageStore);
        } else {
            throw new RuntimeException(format("queue type %s is not supported.", cqType.toString()));
        }
    }

    private void queueTypeShouldBe(String topic, CQType cqTypeExpected) {
        Optional<TopicConfig> topicConfig = this.messageStore.getTopicConfig(topic);

        CQType cqTypeActual = QueueTypeUtils.getCQType(topicConfig);

        if (!Objects.equals(cqTypeExpected, cqTypeActual)) {
            throw new RuntimeException(format("The queue type of topic: %s should be %s, but is %s", topic, cqTypeExpected, cqTypeActual));
        }
    }

    private ExecutorService buildExecutorService(BlockingQueue<Runnable> blockingQueue, String threadNamePrefix) {
        return ThreadUtils.newThreadPoolExecutor(
            this.messageStore.getBrokerConfig().getRecoverThreadPoolNums(),
            this.messageStore.getBrokerConfig().getRecoverThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            blockingQueue,
            new ThreadFactoryImpl(threadNamePrefix));
    }

    public void recover(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.recover();
    }

    @Override
    public Long getMaxPhyOffsetInConsumeQueue(String topic, int queueId) {
        ConsumeQueueInterface logic = findOrCreateConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getMaxPhysicOffset();
        }
        return null;
    }

    @Override
    public long getMaxPhyOffsetInConsumeQueue() {
        long maxPhysicOffset = -1L;
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueueInterface logic : maps.values()) {
                if (logic.getMaxPhysicOffset() > maxPhysicOffset) {
                    maxPhysicOffset = logic.getMaxPhysicOffset();
                }
            }
        }
        return maxPhysicOffset;
    }

    @Override
    public long getMinOffsetInQueue(String topic, int queueId) {
        ConsumeQueueInterface logic = findOrCreateConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getMinOffsetInQueue();
        }

        return -1;
    }

    public void checkSelf(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.checkSelf();
    }

    @Override
    public void checkSelf() {
        for (Map.Entry<String, ConcurrentMap<Integer, ConsumeQueueInterface>> topicEntry : this.consumeQueueTable.entrySet()) {
            for (Map.Entry<Integer, ConsumeQueueInterface> cqEntry : topicEntry.getValue().entrySet()) {
                this.checkSelf(cqEntry.getValue());
            }
        }
    }

    @Override
    public boolean flush(ConsumeQueueInterface consumeQueue, int flushLeastPages) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.flush(flushLeastPages);
    }

    @Override
    public void destroy(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.destroy();
    }

    @Override
    public int deleteExpiredFile(ConsumeQueueInterface consumeQueue, long minCommitLogPos) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.deleteExpiredFile(minCommitLogPos);
    }

    public void truncateDirtyLogicFiles(ConsumeQueueInterface consumeQueue, long phyOffset) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.truncateDirtyLogicFiles(phyOffset);
    }

    public void swapMap(ConsumeQueueInterface consumeQueue, int reserveNum, long forceSwapIntervalMs,
        long normalSwapIntervalMs) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.swapMap(reserveNum, forceSwapIntervalMs, normalSwapIntervalMs);
    }

    public void cleanSwappedMap(ConsumeQueueInterface consumeQueue, long forceCleanSwapIntervalMs) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.cleanSwappedMap(forceCleanSwapIntervalMs);
    }

    @Override
    public boolean isFirstFileAvailable(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.isFirstFileAvailable();
    }

    @Override
    public boolean isFirstFileExist(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.isFirstFileExist();
    }

    /**根据topic 和 queueId 找到对应的consumequeue，如果找不到的话会进行创建*/
    @Override
    public ConsumeQueueInterface findOrCreateConsumeQueue(String topic, int queueId) {
        ConcurrentMap<Integer, ConsumeQueueInterface> map = consumeQueueTable.get(topic);
        if (null == map) { //如果没有，则会创建一个新的元素放进去
            ConcurrentMap<Integer, ConsumeQueueInterface> newMap = new ConcurrentHashMap<>(128);
            ConcurrentMap<Integer, ConsumeQueueInterface> oldMap = consumeQueueTable.putIfAbsent(topic, newMap);
            if (oldMap != null) { //有可能在我们创建好的时候，其他线程已经创建了，那么就返回已经被创建
                map = oldMap;
            } else {
                map = newMap;
            }
        }

        ConsumeQueueInterface logic = map.get(queueId); //获取queueId对应的队列
        if (logic != null) {
            return logic;
        }

        ConsumeQueueInterface newLogic;

        Optional<TopicConfig> topicConfig = this.messageStore.getTopicConfig(topic);
        // TODO maybe the topic has been deleted.
        if (Objects.equals(CQType.BatchCQ, QueueTypeUtils.getCQType(topicConfig))) {
            newLogic = new BatchConsumeQueue(
                topic,
                queueId,
                getStorePathBatchConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                this.messageStoreConfig.getMapperFileSizeBatchConsumeQueue(),
                this.messageStore);
        } else {
            newLogic = new ConsumeQueue(
                topic,
                queueId,
                getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                this.messageStoreConfig.getMappedFileSizeConsumeQueue(),
                this.messageStore);
        }

        ConsumeQueueInterface oldLogic = map.putIfAbsent(queueId, newLogic);
        if (oldLogic != null) {
            logic = oldLogic;
        } else {
            logic = newLogic;
        }

        return logic;
    }

    public void setBatchTopicQueueTable(ConcurrentMap<String, Long> batchTopicQueueTable) {
        this.queueOffsetOperator.setBatchTopicQueueTable(batchTopicQueueTable);
    }

    public void updateQueueOffset(String topic, int queueId, long offset) {
        String topicQueueKey = topic + "-" + queueId;
        this.queueOffsetOperator.updateQueueOffset(topicQueueKey, offset);
    }

    /*将从文件加载的消息队列存储到consumeQueueTable*/
    private void putConsumeQueue(final String topic, final int queueId, final ConsumeQueueInterface consumeQueue) {
        ConcurrentMap<Integer/* queueId */, ConsumeQueueInterface> map = this.consumeQueueTable.get(topic);
        if (null == map) {
            map = new ConcurrentHashMap<>();
            map.put(queueId, consumeQueue);
            this.consumeQueueTable.put(topic, map);
        } else {
            map.put(queueId, consumeQueue);
        }
    }

    @Override
    public void recoverOffsetTable(long minPhyOffset) {
        ConcurrentMap<String, Long> cqOffsetTable = new ConcurrentHashMap<>(1024);
        ConcurrentMap<String, Long> bcqOffsetTable = new ConcurrentHashMap<>(1024);

        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueueInterface logic : maps.values()) {
                String key = logic.getTopic() + "-" + logic.getQueueId();

                long maxOffsetInQueue = logic.getMaxOffsetInQueue();
                if (Objects.equals(CQType.BatchCQ, logic.getCQType())) {
                    bcqOffsetTable.put(key, maxOffsetInQueue);
                } else {
                    cqOffsetTable.put(key, maxOffsetInQueue);
                }

                this.correctMinOffset(logic, minPhyOffset);
            }
        }

        // Correct unSubmit consumeOffset
        if (messageStoreConfig.isDuplicationEnable() || messageStore.getBrokerConfig().isEnableControllerMode()) {
            compensateForHA(cqOffsetTable);
        }

        this.setTopicQueueTable(cqOffsetTable);
        this.setBatchTopicQueueTable(bcqOffsetTable);
    }
    private void compensateForHA(ConcurrentMap<String, Long> cqOffsetTable) {
        SelectMappedBufferResult lastBuffer = null;
        long startReadOffset = messageStore.getCommitLog().getConfirmOffset() == -1 ? 0 : messageStore.getCommitLog().getConfirmOffset();
        log.info("Correct unsubmitted offset...StartReadOffset = {}", startReadOffset);
        while ((lastBuffer = messageStore.selectOneMessageByOffset(startReadOffset)) != null) {
            try {
                if (lastBuffer.getStartOffset() > startReadOffset) {
                    startReadOffset = lastBuffer.getStartOffset();
                    continue;
                }

                ByteBuffer bb = lastBuffer.getByteBuffer();
                int magicCode = bb.getInt(bb.position() + 4);
                if (magicCode == CommitLog.BLANK_MAGIC_CODE) {
                    startReadOffset += bb.getInt(bb.position());
                    continue;
                } else if (magicCode != MessageDecoder.MESSAGE_MAGIC_CODE) {
                    throw new RuntimeException("Unknown magicCode: " + magicCode);
                }

                lastBuffer.getByteBuffer().mark();
                DispatchRequest dispatchRequest = messageStore.getCommitLog().checkMessageAndReturnSize(lastBuffer.getByteBuffer(), true, messageStoreConfig.isDuplicationEnable(), true);
                if (!dispatchRequest.isSuccess())
                    break;
                lastBuffer.getByteBuffer().reset();

                MessageExt msg = MessageDecoder.decode(lastBuffer.getByteBuffer(), true, false, false, false, true);
                if (msg == null)
                    break;

                String key = msg.getTopic() + "-" + msg.getQueueId();
                cqOffsetTable.put(key, msg.getQueueOffset() + 1);
                startReadOffset += msg.getStoreSize();
                log.info("Correcting. Key:{}, start read Offset: {}", key, startReadOffset);
            } finally {
                if (lastBuffer != null)
                    lastBuffer.release();
            }
        }
    }

    @Override
    public void destroy() {
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueueInterface logic : maps.values()) {
                this.destroy(logic);
            }
        }
    }

    @Override
    public void cleanExpired(long minCommitLogOffset) {
        Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueueInterface>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, ConcurrentMap<Integer, ConsumeQueueInterface>> next = it.next();
            String topic = next.getKey();
            if (!TopicValidator.isSystemTopic(topic)) {
                ConcurrentMap<Integer, ConsumeQueueInterface> queueTable = next.getValue();
                Iterator<Map.Entry<Integer, ConsumeQueueInterface>> itQT = queueTable.entrySet().iterator();
                while (itQT.hasNext()) {
                    Map.Entry<Integer, ConsumeQueueInterface> nextQT = itQT.next();
                    long maxCLOffsetInConsumeQueue = nextQT.getValue().getLastOffset();

                    if (maxCLOffsetInConsumeQueue == -1) {
                        log.warn("maybe ConsumeQueue was created just now. topic={} queueId={} maxPhysicOffset={} minLogicOffset={}.",
                            nextQT.getValue().getTopic(),
                            nextQT.getValue().getQueueId(),
                            nextQT.getValue().getMaxPhysicOffset(),
                            nextQT.getValue().getMinLogicOffset());
                    } else if (maxCLOffsetInConsumeQueue < minCommitLogOffset) {
                        log.info(
                            "cleanExpiredConsumerQueue: {} {} consumer queue destroyed, minCommitLogOffset: {} maxCLOffsetInConsumeQueue: {}",
                            topic,
                            nextQT.getKey(),
                            minCommitLogOffset,
                            maxCLOffsetInConsumeQueue);

                        removeTopicQueueTable(nextQT.getValue().getTopic(),
                            nextQT.getValue().getQueueId());

                        this.destroy(nextQT.getValue());
                        itQT.remove();
                    }
                }

                if (queueTable.isEmpty()) {
                    log.info("cleanExpiredConsumerQueue: {},topic destroyed", topic);
                    it.remove();
                }
            }
        }
    }

    @Override
    public void truncateDirty(long offsetToTruncate) {
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueueInterface logic : maps.values()) {
                this.truncateDirtyLogicFiles(logic, offsetToTruncate);
            }
        }
    }

    @Override
    public long getTotalSize() {
        long totalSize = 0;
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueueInterface logic : maps.values()) {
                totalSize += logic.getTotalSize();
            }
        }
        return totalSize;
    }
}
