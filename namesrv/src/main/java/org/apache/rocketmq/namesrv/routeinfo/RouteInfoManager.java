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

import com.google.common.collect.Sets;
import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.ConcurrentHashMapUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigAndMappingSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.body.TopicList;
import org.apache.rocketmq.remoting.protocol.header.NotifyMinBrokerIdChangeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.UnRegisterBrokerRequestHeader;
import org.apache.rocketmq.remoting.protocol.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo;

/**NameServer 的主要职责是维护集群的元数据（如 Broker 地址、主题分布等），而 RouteInfoManager 就是实现这些功能的核心类。*/
public class RouteInfoManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);
    private final static long DEFAULT_BROKER_CHANNEL_EXPIRED_TIME = 1000 * 60 * 2;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    /*消息队列的路由信息，根据这个属性进行负载均衡。。键：topic名称；值：brokerName——>该brokerName中的QueueData信息*/
    private final Map<String/* topic */, Map<String, QueueData>> topicQueueTable;
    private final Map<String/* brokerName */, BrokerData> brokerAddrTable;  //broker名称到broker的基本信息，包括：名称->所属集群名、主备broker地址(该broker集群的所有broker实例)
    private final Map<String/* clusterName */, Set<String/* brokerName */>> clusterAddrTable; //集群名称和broker名称的映射，用于快速查找broker
    private final Map<BrokerAddrInfo/* brokerAddr */, BrokerLiveInfo> brokerLiveTable;  //broker的状态信息，每次收到心跳响应时会更新此信息
    private final Map<BrokerAddrInfo/* brokerAddr */, List<String>/* Filter Server */> filterServerTable;   //broker上的filterserver列表，实现类模式消息过滤
    private final Map<String/* topic */, Map<String/*brokerName*/, TopicQueueMappingInfo>> topicQueueMappingInfoTable;

    private final BatchUnregistrationService unRegisterService;

    private final NamesrvController namesrvController;
    private final NamesrvConfig namesrvConfig;

    public RouteInfoManager(final NamesrvConfig namesrvConfig, NamesrvController namesrvController) {
        this.topicQueueTable = new ConcurrentHashMap<>(1024);
        this.brokerAddrTable = new ConcurrentHashMap<>(128);
        this.clusterAddrTable = new ConcurrentHashMap<>(32);
        this.brokerLiveTable = new ConcurrentHashMap<>(256);
        this.filterServerTable = new ConcurrentHashMap<>(256);
        this.topicQueueMappingInfoTable = new ConcurrentHashMap<>(1024);
        this.unRegisterService = new BatchUnregistrationService(this, namesrvConfig);
        this.namesrvConfig = namesrvConfig;
        this.namesrvController = namesrvController;
    }

    public void start() {
        this.unRegisterService.start();
    }

    public void shutdown() {
        this.unRegisterService.shutdown(true);
    }

    public boolean submitUnRegisterBrokerRequest(UnRegisterBrokerRequestHeader unRegisterRequest) {
        return this.unRegisterService.submit(unRegisterRequest);
    }

    // For test only
    int blockedUnRegisterRequests() {
        return this.unRegisterService.queueLength();
    }

    public ClusterInfo getAllClusterInfo() {
        ClusterInfo clusterInfoSerializeWrapper = new ClusterInfo();
        clusterInfoSerializeWrapper.setBrokerAddrTable(this.brokerAddrTable);
        clusterInfoSerializeWrapper.setClusterAddrTable(this.clusterAddrTable);
        return clusterInfoSerializeWrapper;
    }

    public void registerTopic(final String topic, List<QueueData> queueDatas) {
        if (queueDatas == null || queueDatas.isEmpty()) {
            return;
        }

        try {
            this.lock.writeLock().lockInterruptibly();
            if (this.topicQueueTable.containsKey(topic)) {
                Map<String, QueueData> queueDataMap  = this.topicQueueTable.get(topic);
                for (QueueData queueData : queueDatas) {
                    if (!this.brokerAddrTable.containsKey(queueData.getBrokerName())) {
                        log.warn("Register topic contains illegal broker, {}, {}", topic, queueData);
                        return;
                    }
                    queueDataMap.put(queueData.getBrokerName(), queueData);
                }
                log.info("Topic route already exist.{}, {}", topic, this.topicQueueTable.get(topic));
            } else {
                // check and construct queue data map
                Map<String, QueueData> queueDataMap = new HashMap<>();
                for (QueueData queueData : queueDatas) {
                    if (!this.brokerAddrTable.containsKey(queueData.getBrokerName())) {
                        log.warn("Register topic contains illegal broker, {}, {}", topic, queueData);
                        return;
                    }
                    queueDataMap.put(queueData.getBrokerName(), queueData);
                }

                this.topicQueueTable.put(topic, queueDataMap);
                log.info("Register topic route:{}, {}", topic, queueDatas);
            }
        } catch (Exception e) {
            log.error("registerTopic Exception", e);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public void deleteTopic(final String topic) {
        try {
            this.lock.writeLock().lockInterruptibly();
            this.topicQueueTable.remove(topic);
        } catch (Exception e) {
            log.error("deleteTopic Exception", e);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public void deleteTopic(final String topic, final String clusterName) {
        try {
            this.lock.writeLock().lockInterruptibly();
            //get all the brokerNames fot the specified cluster
            Set<String> brokerNames = this.clusterAddrTable.get(clusterName);
            if (brokerNames == null || brokerNames.isEmpty()) {
                return;
            }
            //get the store information for single topic
            Map<String, QueueData> queueDataMap = this.topicQueueTable.get(topic);
            if (queueDataMap != null) {
                for (String brokerName : brokerNames) {
                    final QueueData removedQD = queueDataMap.remove(brokerName);
                    if (removedQD != null) {
                        log.info("deleteTopic, remove one broker's topic {} {} {}", brokerName, topic, removedQD);
                    }
                }
                if (queueDataMap.isEmpty()) {
                    log.info("deleteTopic, remove the topic all queue {} {}", clusterName, topic);
                    this.topicQueueTable.remove(topic);
                }
            }
        } catch (Exception e) {
            log.error("deleteTopic Exception", e);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public TopicList getAllTopicList() {
        TopicList topicList = new TopicList();
        try {
            this.lock.readLock().lockInterruptibly();
            topicList.getTopicList().addAll(this.topicQueueTable.keySet());
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        } finally {
            this.lock.readLock().unlock();
        }

        return topicList;
    }

    public RegisterBrokerResult registerBroker(
        final String clusterName,
        final String brokerAddr,
        final String brokerName,
        final long brokerId,
        final String haServerAddr,
        final String zoneName,
        final Long timeoutMillis,
        final TopicConfigSerializeWrapper topicConfigWrapper,
        final List<String> filterServerList,
        final Channel channel) {
        return registerBroker(clusterName, brokerAddr, brokerName, brokerId, haServerAddr, zoneName, timeoutMillis, false, topicConfigWrapper, filterServerList, channel);
    }

    public RegisterBrokerResult registerBroker(
        final String clusterName,
        final String brokerAddr,
        final String brokerName,
        final long brokerId,
        final String haServerAddr,
        final String zoneName,
        final Long timeoutMillis,
        final Boolean enableActingMaster,
        final TopicConfigSerializeWrapper topicConfigWrapper,
        final List<String> filterServerList,
        final Channel channel) {
        RegisterBrokerResult result = new RegisterBrokerResult();
        try {
            this.lock.writeLock().lockInterruptibly();

            //init or update the cluster info
            Set<String> brokerNames = ConcurrentHashMapUtils.computeIfAbsent((ConcurrentHashMap<String, Set<String>>) this.clusterAddrTable, clusterName, k -> new HashSet<>());
            brokerNames.add(brokerName);

            boolean registerFirst = false;
            //根据brokerName获取BrokerData，如果获取不到则标识需要新建
            BrokerData brokerData = this.brokerAddrTable.get(brokerName);
            if (null == brokerData) {
                registerFirst = true;
                brokerData = new BrokerData(clusterName, brokerName, new HashMap<>());
                this.brokerAddrTable.put(brokerName, brokerData);
            }
            //根据enableActingMaster属性判断是不是旧版本的broker。根据是否旧版本broker，设置brokerData的enableActingMaster属性。
            boolean isOldVersionBroker = enableActingMaster == null;
            brokerData.setEnableActingMaster(!isOldVersionBroker && enableActingMaster);
            brokerData.setZoneName(zoneName);

            //从brokerData拿到这个集群所有的broker地址。并对这里拿到的map进行筛选————目的是排除冲突的broker(删除地址相等，但是brokerId不相等的映射)
            Map<Long, String> brokerAddrsMap = brokerData.getBrokerAddrs();

            boolean isMinBrokerIdChanged = false;
            long prevMinBrokerId = 0;
            if (!brokerAddrsMap.isEmpty()) {
                prevMinBrokerId = Collections.min(brokerAddrsMap.keySet());
            }

            if (brokerId < prevMinBrokerId) {
                isMinBrokerIdChanged = true;
            }

            //Switch slave to master: first remove <1, IP:PORT> in namesrv, then add <0, IP:PORT>
            //The same IP:PORT must only have one record in brokerAddrTable
            brokerAddrsMap.entrySet().removeIf(item -> null != brokerAddr && brokerAddr.equals(item.getValue()) && brokerId != item.getKey());

            //If Local brokerId stateVersion bigger than the registering one,
            String oldBrokerAddr = brokerAddrsMap.get(brokerId);
            if (null != oldBrokerAddr && !oldBrokerAddr.equals(brokerAddr)) {
                BrokerLiveInfo oldBrokerInfo = brokerLiveTable.get(new BrokerAddrInfo(clusterName, oldBrokerAddr));

                if (null != oldBrokerInfo) {
                    long oldStateVersion = oldBrokerInfo.getDataVersion().getStateVersion();
                    long newStateVersion = topicConfigWrapper.getDataVersion().getStateVersion();
                    if (oldStateVersion > newStateVersion) {
                        log.warn("Registered Broker conflicts with the existed one, just ignore.: Cluster:{}, BrokerName:{}, BrokerId:{}, " +
                                "Old BrokerAddr:{}, Old Version:{}, New BrokerAddr:{}, New Version:{}.",
                            clusterName, brokerName, brokerId, oldBrokerAddr, oldStateVersion, brokerAddr, newStateVersion);
                        //Remove the rejected brokerAddr from brokerLiveTable.
                        brokerLiveTable.remove(new BrokerAddrInfo(clusterName, brokerAddr));
                        return result;
                    }
                }
            }
            //情况2：如果brokerAddrTable中已经存在这个brokerId，并且brokerAddrTable中这个brokerId对应的topicConfigTable只有一个topic，则返回null
            if (!brokerAddrsMap.containsKey(brokerId) && topicConfigWrapper.getTopicConfigTable().size() == 1) {    //为什么？？
                log.warn("Can't register topicConfigWrapper={} because broker[{}]={} has not registered.",
                    topicConfigWrapper.getTopicConfigTable(), brokerId, brokerAddr);
                return null;
            }

            String oldAddr = brokerAddrsMap.put(brokerId, brokerAddr);
            registerFirst = registerFirst || (StringUtils.isEmpty(oldAddr));

            boolean isMaster = MixAll.MASTER_ID == brokerId;

            boolean isPrimeSlave = !isOldVersionBroker && !isMaster
                && brokerId == Collections.min(brokerAddrsMap.keySet());

            if (null != topicConfigWrapper && (isMaster || isPrimeSlave)) {

                ConcurrentMap<String, TopicConfig> tcTable =
                    topicConfigWrapper.getTopicConfigTable();

                if (tcTable != null) {

                    TopicConfigAndMappingSerializeWrapper mappingSerializeWrapper = TopicConfigAndMappingSerializeWrapper.from(topicConfigWrapper);
                    Map<String, TopicQueueMappingInfo> topicQueueMappingInfoMap = mappingSerializeWrapper.getTopicQueueMappingInfoMap();

                    // Delete the topics that don't exist in tcTable from the current broker
                    // Static topic is not supported currently
                    if (namesrvConfig.isDeleteTopicWithBrokerRegistration() && topicQueueMappingInfoMap.isEmpty()) {
                        final Set<String> oldTopicSet = topicSetOfBrokerName(brokerName);
                        final Set<String> newTopicSet = tcTable.keySet();
                        final Sets.SetView<String> toDeleteTopics = Sets.difference(oldTopicSet, newTopicSet);
                        for (final String toDeleteTopic : toDeleteTopics) {
                            Map<String, QueueData> queueDataMap = topicQueueTable.get(toDeleteTopic);
                            final QueueData removedQD = queueDataMap.remove(brokerName);
                            if (removedQD != null) {
                                log.info("deleteTopic, remove one broker's topic {} {} {}", brokerName, toDeleteTopic, removedQD);
                            }

                            if (queueDataMap.isEmpty()) {
                                log.info("deleteTopic, remove the topic all queue {}", toDeleteTopic);
                                topicQueueTable.remove(toDeleteTopic);
                            }
                        }
                    }

                    for (Map.Entry<String, TopicConfig> entry : tcTable.entrySet()) {
                        if (registerFirst || this.isTopicConfigChanged(clusterName, brokerAddr,
                            topicConfigWrapper.getDataVersion(), brokerName,
                            entry.getValue().getTopicName())) {
                            final TopicConfig topicConfig = entry.getValue();
                            // In Slave Acting Master mode, Namesrv will regard the surviving Slave with the smallest brokerId as the "agent" Master, and modify the brokerPermission to read-only.
                            if (isPrimeSlave && brokerData.isEnableActingMaster()) {
                                // Wipe write perm for prime slave
                                topicConfig.setPerm(topicConfig.getPerm() & (~PermName.PERM_WRITE));
                            }
                            this.createAndUpdateQueueData(brokerName, topicConfig);
                        }
                    }

                    if (this.isBrokerTopicConfigChanged(clusterName, brokerAddr, topicConfigWrapper.getDataVersion()) || registerFirst) {
                        //the topicQueueMappingInfoMap should never be null, but can be empty
                        for (Map.Entry<String, TopicQueueMappingInfo> entry : topicQueueMappingInfoMap.entrySet()) {
                            if (!topicQueueMappingInfoTable.containsKey(entry.getKey())) {
                                topicQueueMappingInfoTable.put(entry.getKey(), new HashMap<>());
                            }
                            //Note asset brokerName equal entry.getValue().getBname()
                            //here use the mappingDetail.bname
                            topicQueueMappingInfoTable.get(entry.getKey()).put(entry.getValue().getBname(), entry.getValue());
                        }
                    }
                }
            }

            BrokerAddrInfo brokerAddrInfo = new BrokerAddrInfo(clusterName, brokerAddr);
            BrokerLiveInfo prevBrokerLiveInfo = this.brokerLiveTable.put(brokerAddrInfo,
                new BrokerLiveInfo(
                    System.currentTimeMillis(),
                    timeoutMillis == null ? DEFAULT_BROKER_CHANNEL_EXPIRED_TIME : timeoutMillis,
                    topicConfigWrapper == null ? new DataVersion() : topicConfigWrapper.getDataVersion(),
                    channel,
                    haServerAddr));
            if (null == prevBrokerLiveInfo) {
                log.info("new broker registered, {} HAService: {}", brokerAddrInfo, haServerAddr);
            }

            if (filterServerList != null) {
                if (filterServerList.isEmpty()) {
                    this.filterServerTable.remove(brokerAddrInfo);
                } else {
                    this.filterServerTable.put(brokerAddrInfo, filterServerList);
                }
            }

            if (MixAll.MASTER_ID != brokerId) {
                String masterAddr = brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
                if (masterAddr != null) {
                    BrokerAddrInfo masterAddrInfo = new BrokerAddrInfo(clusterName, masterAddr);
                    BrokerLiveInfo masterLiveInfo = this.brokerLiveTable.get(masterAddrInfo);
                    if (masterLiveInfo != null) {
                        result.setHaServerAddr(masterLiveInfo.getHaServerAddr());
                        result.setMasterAddr(masterAddr);
                    }
                }
            }

            if (isMinBrokerIdChanged && namesrvConfig.isNotifyMinBrokerIdChanged()) {
                notifyMinBrokerIdChanged(brokerAddrsMap, null,
                    this.brokerLiveTable.get(brokerAddrInfo).getHaServerAddr());
            }
        } catch (Exception e) {
            log.error("registerBroker Exception", e);
        } finally {
            this.lock.writeLock().unlock();
        }

        return result;
    }

    private Set<String> topicSetOfBrokerName(final String brokerName) {
        Set<String> topicOfBroker = new HashSet<>();
        for (final Entry<String, Map<String, QueueData>> entry : this.topicQueueTable.entrySet()) {
            if (entry.getValue().containsKey(brokerName)) {
                topicOfBroker.add(entry.getKey());
            }
        }
        return topicOfBroker;
    }

    public BrokerMemberGroup getBrokerMemberGroup(String clusterName, String brokerName) {
        BrokerMemberGroup groupMember = new BrokerMemberGroup(clusterName, brokerName);
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                final BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                if (brokerData != null) {
                    groupMember.getBrokerAddrs().putAll(brokerData.getBrokerAddrs());
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("Get broker member group exception", e);
        }
        return groupMember;
    }

    public boolean isBrokerTopicConfigChanged(final String clusterName, final String brokerAddr,
        final DataVersion dataVersion) {
        DataVersion prev = queryBrokerTopicConfig(clusterName, brokerAddr);
        return null == prev || !prev.equals(dataVersion);
    }

    public boolean isTopicConfigChanged(final String clusterName, final String brokerAddr,
        final DataVersion dataVersion, String brokerName, String topic) {
        boolean isChange = isBrokerTopicConfigChanged(clusterName, brokerAddr, dataVersion);
        if (isChange) {
            return true;
        }
        final Map<String, QueueData> queueDataMap = this.topicQueueTable.get(topic);
        if (queueDataMap == null || queueDataMap.isEmpty()) {
            return true;
        }

        // The topicQueueTable already contains the broker
        return !queueDataMap.containsKey(brokerName);
    }

    public DataVersion queryBrokerTopicConfig(final String clusterName, final String brokerAddr) {
        BrokerAddrInfo addrInfo = new BrokerAddrInfo(clusterName, brokerAddr);
        BrokerLiveInfo prev = this.brokerLiveTable.get(addrInfo);
        if (prev != null) {
            return prev.getDataVersion();
        }
        return null;
    }

    public void updateBrokerInfoUpdateTimestamp(final String clusterName, final String brokerAddr) {
        BrokerAddrInfo addrInfo = new BrokerAddrInfo(clusterName, brokerAddr);
        BrokerLiveInfo prev = this.brokerLiveTable.get(addrInfo);
        if (prev != null) {
            prev.setLastUpdateTimestamp(System.currentTimeMillis());
        }
    }

    private void createAndUpdateQueueData(final String brokerName, final TopicConfig topicConfig) {
        QueueData queueData = new QueueData();
        queueData.setBrokerName(brokerName);
        queueData.setWriteQueueNums(topicConfig.getWriteQueueNums());
        queueData.setReadQueueNums(topicConfig.getReadQueueNums());
        queueData.setPerm(topicConfig.getPerm());
        queueData.setTopicSysFlag(topicConfig.getTopicSysFlag());

        Map<String, QueueData> queueDataMap = this.topicQueueTable.get(topicConfig.getTopicName());
        if (null == queueDataMap) {
            queueDataMap = new HashMap<>();
            queueDataMap.put(brokerName, queueData);
            this.topicQueueTable.put(topicConfig.getTopicName(), queueDataMap);
            log.info("new topic registered, {} {}", topicConfig.getTopicName(), queueData);
        } else {
            final QueueData existedQD = queueDataMap.get(brokerName);
            if (existedQD == null) {
                queueDataMap.put(brokerName, queueData);
            } else if (!existedQD.equals(queueData)) {
                log.info("topic changed, {} OLD: {} NEW: {}", topicConfig.getTopicName(), existedQD,
                    queueData);
                queueDataMap.put(brokerName, queueData);
            }
        }
    }

    public int wipeWritePermOfBrokerByLock(final String brokerName) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                return operateWritePermOfBroker(brokerName, RequestCode.WIPE_WRITE_PERM_OF_BROKER);
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("wipeWritePermOfBrokerByLock Exception", e);
        }

        return 0;
    }

    public int addWritePermOfBrokerByLock(final String brokerName) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                return operateWritePermOfBroker(brokerName, RequestCode.ADD_WRITE_PERM_OF_BROKER);
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("addWritePermOfBrokerByLock Exception", e);
        }
        return 0;
    }

    private int operateWritePermOfBroker(final String brokerName, final int requestCode) {
        int topicCnt = 0;

        for (Entry<String, Map<String, QueueData>> entry : this.topicQueueTable.entrySet()) {
            Map<String, QueueData> qdMap = entry.getValue();

            final QueueData qd = qdMap.get(brokerName);
            if (qd == null) {
                continue;
            }
            int perm = qd.getPerm();
            switch (requestCode) {
                case RequestCode.WIPE_WRITE_PERM_OF_BROKER:
                    perm &= ~PermName.PERM_WRITE;
                    break;
                case RequestCode.ADD_WRITE_PERM_OF_BROKER:
                    perm = PermName.PERM_READ | PermName.PERM_WRITE;
                    break;
            }
            qd.setPerm(perm);
            topicCnt++;
        }
        return topicCnt;
    }

    public void unregisterBroker(
        final String clusterName,
        final String brokerAddr,
        final String brokerName,
        final long brokerId) {
        UnRegisterBrokerRequestHeader unRegisterBrokerRequest = new UnRegisterBrokerRequestHeader();
        unRegisterBrokerRequest.setClusterName(clusterName);
        unRegisterBrokerRequest.setBrokerAddr(brokerAddr);
        unRegisterBrokerRequest.setBrokerName(brokerName);
        unRegisterBrokerRequest.setBrokerId(brokerId);

        unRegisterBroker(Sets.newHashSet(unRegisterBrokerRequest));
    }

    /**[]：根据set集合(set集合中是一个个的Broker注销的请求)，注销集合中提到的Broker注销请求*/
    public void unRegisterBroker(Set<UnRegisterBrokerRequestHeader> unRegisterRequests) {
        try {
            Set<String> removedBroker = new HashSet<>();
            Set<String> reducedBroker = new HashSet<>();
            Map<String, BrokerStatusChangeInfo> needNotifyBrokerMap = new HashMap<>();

            this.lock.writeLock().lockInterruptibly();  //拿到写锁
            for (final UnRegisterBrokerRequestHeader unRegisterRequest : unRegisterRequests) {
                final String brokerName = unRegisterRequest.getBrokerName();
                final String clusterName = unRegisterRequest.getClusterName();
                final String brokerAddr = unRegisterRequest.getBrokerAddr();

                BrokerAddrInfo brokerAddrInfo = new BrokerAddrInfo(clusterName, brokerAddr);
                //操作1：更新brokerLiveTable————删除指定的broker信息
                BrokerLiveInfo brokerLiveInfo = this.brokerLiveTable.remove(brokerAddrInfo);
                log.info("unregisterBroker, remove from brokerLiveTable {}, {}",
                    brokerLiveInfo != null ? "OK" : "Failed",
                    brokerAddrInfo
                );
                //操作2：更新filterServerTable————删除指定的broker信息
                this.filterServerTable.remove(brokerAddrInfo);
                /*操作3：下面的if语句块的作用是更新brokerAddrTable————删除指定的broker信息
                * 详细的来说：
                *       遍历HashMap<String, BrokerData> brokerAddrTable，从BrokerData的HashMap<Long, String>
                brokerAddrs中，找到具体的Broker，从BrokerData中将其移除。如果移除后在BrokerData中不再包含其他Broker，
                * 则在brokerAddrTable中移除该brokerName对应的条目
                * */
                boolean removeBrokerName = false;
                boolean isMinBrokerIdChanged = false;
                BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                if (null != brokerData) {   //确保这个对象不是null（下面是根据brokerAddrTable得到的value来进行操作）
                    if (!brokerData.getBrokerAddrs().isEmpty() &&
                        unRegisterRequest.getBrokerId().equals(Collections.min(brokerData.getBrokerAddrs().keySet()))) {
                        isMinBrokerIdChanged = true;
                    }
                    boolean removed = brokerData.getBrokerAddrs().entrySet().removeIf(item -> item.getValue().equals(brokerAddr));  //从broker集群中删除当前的这个broker
                    log.info("unregisterBroker, remove addr from brokerAddrTable {}, {}",
                        removed ? "OK" : "Failed",
                        brokerAddrInfo
                    );
                    if (brokerData.getBrokerAddrs().isEmpty()) {  //如果broker集群中已经没有broker了，则从brokerAddrTable中删除这个broker集群
                        this.brokerAddrTable.remove(brokerName);
                        log.info("unregisterBroker, remove name from brokerAddrTable OK, {}",
                            brokerName
                        );

                        removeBrokerName = true;
                    } else if (isMinBrokerIdChanged) {
                        needNotifyBrokerMap.put(brokerName, new BrokerStatusChangeInfo(
                            brokerData.getBrokerAddrs(), brokerAddr, null));
                    }
                }
                /*操作4：更新clusterAddrTable————删除指定的broker信息
                * 详细的来说：
                *       根据BrokerName，从clusterAddrTable中找到Broker并将其从集群中移除。如果移除后，集群中不包含任
                * 何Broker，则将该集群从clusterAddrTable中移除*/
                if (removeBrokerName) {
                    Set<String> nameSet = this.clusterAddrTable.get(clusterName);
                    if (nameSet != null) {  //源码中在每一次对某东西做操作时，先确保该东西不是null
                        boolean removed = nameSet.remove(brokerName);
                        log.info("unregisterBroker, remove name from clusterAddrTable {}, {}",
                            removed ? "OK" : "Failed",
                            brokerName);

                        if (nameSet.isEmpty()) {  //如果clusterAddrTable中已经没有broker集群了，则从clusterAddrTable中删除这个cluster
                            this.clusterAddrTable.remove(clusterName);
                            log.info("unregisterBroker, remove cluster from clusterAddrTable {}",
                                clusterName
                            );
                        }
                    }
                    removedBroker.add(brokerName);
                } else {
                    reducedBroker.add(brokerName);
                }
            }
            /*操作5：根据BrokerName，遍历所有主题的队列，如果队列中包含当前Broker的队列，则移除，如果topic只包含待移除Broker的队
                列，从路由表中删除该topic*/
            cleanTopicByUnRegisterRequests(removedBroker, reducedBroker);

            if (!needNotifyBrokerMap.isEmpty() && namesrvConfig.isNotifyMinBrokerIdChanged()) {
                notifyMinBrokerIdChanged(needNotifyBrokerMap);
            }
        } catch (Exception e) {
            log.error("unregisterBroker Exception", e);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    private void cleanTopicByUnRegisterRequests(Set<String> removedBroker, Set<String> reducedBroker) {
        Iterator<Entry<String, Map<String, QueueData>>> itMap = this.topicQueueTable.entrySet().iterator();
        while (itMap.hasNext()) {
            Entry<String, Map<String, QueueData>> entry = itMap.next();

            String topic = entry.getKey();
            Map<String, QueueData> queueDataMap = entry.getValue();

            for (final String brokerName : removedBroker) {
                final QueueData removedQD = queueDataMap.remove(brokerName);
                if (removedQD != null) {
                    log.debug("removeTopicByBrokerName, remove one broker's topic {} {}", topic, removedQD);
                }
            }

            if (queueDataMap.isEmpty()) {
                log.debug("removeTopicByBrokerName, remove the topic all queue {}", topic);
                itMap.remove();
            }

            for (final String brokerName : reducedBroker) {
                final QueueData queueData = queueDataMap.get(brokerName);

                if (queueData != null) {
                    if (this.brokerAddrTable.get(brokerName).isEnableActingMaster()) {
                        // Master has been unregistered, wipe the write perm
                        if (isNoMasterExists(brokerName)) {
                            queueData.setPerm(queueData.getPerm() & (~PermName.PERM_WRITE));
                        }
                    }
                }
            }
        }
    }

    private boolean isNoMasterExists(String brokerName) {
        final BrokerData brokerData = this.brokerAddrTable.get(brokerName);
        if (brokerData == null) {
            return true;
        }

        if (brokerData.getBrokerAddrs().size() == 0) {
            return true;
        }

        return Collections.min(brokerData.getBrokerAddrs().keySet()) > 0;
    }

    /**[]：用于根据主题（Topic）从路由表中提取路由信息（TopicRouteData）
     * */
    public TopicRouteData pickupTopicRouteData(final String topic) {
        /*初始化一个TopicRouteData作为返回结果*/
        TopicRouteData topicRouteData = new TopicRouteData();
        boolean foundQueueData = false;
        boolean foundBrokerData = false;
        List<BrokerData> brokerDataList = new LinkedList<>();
        topicRouteData.setBrokerDatas(brokerDataList);
        HashMap<String, List<String>> filterServerMap = new HashMap<>();
        topicRouteData.setFilterServerTable(filterServerMap);

        try {
            this.lock.readLock().lockInterruptibly();
            /*从 topicQueueTable 中获取指定主题的队列分布信息（queueDataMap）*/
            Map<String, QueueData> queueDataMap = this.topicQueueTable.get(topic);
            if (queueDataMap != null) {
                /*如果 queueDataMap 不为空，将队列数据（QueueData）添加到 TopicRouteData 中，
                    并设置 foundQueueData 为 true。*/
                topicRouteData.setQueueDatas(new ArrayList<>(queueDataMap.values()));
                foundQueueData = true;
                /*提取所有队列数据中涉及的 Broker 名称集合（brokerNameSet），以便后续查找对应
                    的 Broker 数据。*/
                Set<String> brokerNameSet = new HashSet<>(queueDataMap.keySet());
                /*遍历 brokerNameSet，从 brokerAddrTable 中查找每个 Broker 的地址信息（BrokerData）。
                    如果找到对应的 BrokerData，将其克隆后添加到 brokerDataList 中，并设置 foundBrokerData
                        为 true。
                    如果存在过滤服务器信息（filterServerTable），则提取对应的过滤服务器列表，并存储到
                        filterServerMap 中。*/
                for (String brokerName : brokerNameSet) {
                    BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                    if (null == brokerData) {
                        continue;
                    }
                    BrokerData brokerDataClone = new BrokerData(brokerData);

                    brokerDataList.add(brokerDataClone);
                    foundBrokerData = true;
                    if (filterServerTable.isEmpty()) {
                        continue;
                    }
                    for (final String brokerAddr : brokerDataClone.getBrokerAddrs().values()) {
                        BrokerAddrInfo brokerAddrInfo = new BrokerAddrInfo(brokerDataClone.getCluster(), brokerAddr);
                        List<String> filterServerList = this.filterServerTable.get(brokerAddrInfo);
                        filterServerMap.put(brokerAddr, filterServerList);
                    }

                }
            }
        } catch (Exception e) {
            log.error("pickupTopicRouteData Exception", e);
        } finally {
            this.lock.readLock().unlock();
        }

        log.debug("pickupTopicRouteData {} {}", topic, topicRouteData);
        /*如果同时找到了队列数据和 Broker 数据，则继续处理；否则返回 null。*/
        if (foundBrokerData && foundQueueData) {

            topicRouteData.setTopicQueueMappingByBroker(this.topicQueueMappingInfoTable.get(topic));
            //如果未启用 Acting Master 功能（isSupportActingMaster()），直接返回路由信息。
            if (!namesrvConfig.isSupportActingMaster()) {
                return topicRouteData;
            }

            if (topic.startsWith(TopicValidator.SYNC_BROKER_MEMBER_GROUP_PREFIX)) {
                return topicRouteData;
            }

            if (topicRouteData.getBrokerDatas().size() == 0 || topicRouteData.getQueueDatas().size() == 0) {
                return topicRouteData;
            }

            boolean needActingMaster = false;
            /*检查是否存在没有主节点的 Broker：
                如果某个 Broker 没有主节点（即 brokerAddrs 中不包含 MASTER_ID），则需要启
                    用 Acting Master。*/
            for (final BrokerData brokerData : topicRouteData.getBrokerDatas()) {
                if (brokerData.getBrokerAddrs().size() != 0
                    && !brokerData.getBrokerAddrs().containsKey(MixAll.MASTER_ID)) {
                    needActingMaster = true;
                    break;
                }
            }
            //如果不需要 Acting Master，直接返回路由信息。
            if (!needActingMaster) {
                return topicRouteData;
            }
            /*遍历所有 Broker 数据，检查是否需要设置 Acting Master：
                如果某个 Broker 没有主节点，并且启用了 Acting Master 功能，则选择最小的 Broker ID
                    作为 Acting Master。将 Acting Master 的地址替换为 MASTER_ID。
                这种设计确保即使主节点不可用，系统仍然能够通过 Acting Master 继续提供服务。*/
            for (final BrokerData brokerData : topicRouteData.getBrokerDatas()) {
                final HashMap<Long, String> brokerAddrs = brokerData.getBrokerAddrs();
                if (brokerAddrs.size() == 0 || brokerAddrs.containsKey(MixAll.MASTER_ID) || !brokerData.isEnableActingMaster()) {
                    continue;
                }

                // No master
                for (final QueueData queueData : topicRouteData.getQueueDatas()) {
                    if (queueData.getBrokerName().equals(brokerData.getBrokerName())) {
                        if (!PermName.isWriteable(queueData.getPerm())) {
                            final Long minBrokerId = Collections.min(brokerAddrs.keySet());
                            final String actingMasterAddr = brokerAddrs.remove(minBrokerId);
                            brokerAddrs.put(MixAll.MASTER_ID, actingMasterAddr);
                        }
                        break;
                    }
                }

            }

            return topicRouteData;
        }

        return null;
    }

    /**【】：用于扫描(通过brokerLiveTable来扫描)并清理不活跃Broker的逻辑，主要目的是检测那些长时间未发送心跳
     * 包的 Broker，并将其从 NameServer 的路由表中移除
     *      具体来说：scanNotActiveBroker在NameServer中每10s执行一次。首先遍历brokerLiveInfo路由表（HashMap），检测
     * BrokerLiveInfo的LastUpdateTimestamp上次收到心跳包的时间，如果超过120s，则认为该Broker已不可用，然后将它移除并关
     * 闭连接，最后删除与该Broker相关的路由信息*/
    public void scanNotActiveBroker() {
        try {
            log.info("start scanNotActiveBroker");
            for (Entry<BrokerAddrInfo, BrokerLiveInfo> next : this.brokerLiveTable.entrySet()) {
                long last = next.getValue().getLastUpdateTimestamp();
                long timeoutMillis = next.getValue().getHeartbeatTimeoutMillis();
                if ((last + timeoutMillis) < System.currentTimeMillis()) {
                    RemotingHelper.closeChannel(next.getValue().getChannel());
                    log.warn("The broker channel expired, {} {}ms", next.getKey(), timeoutMillis);
                    this.onChannelDestroy(next.getKey());
                }
            }
        } catch (Exception e) {
            log.error("scanNotActiveBroker exception", e);
        }
    }

    public void onChannelDestroy(BrokerAddrInfo brokerAddrInfo) {
        UnRegisterBrokerRequestHeader unRegisterRequest = new UnRegisterBrokerRequestHeader();
        boolean needUnRegister = false;
        if (brokerAddrInfo != null) {
            try {
                try {
                    this.lock.readLock().lockInterruptibly();
                    needUnRegister = setupUnRegisterRequest(unRegisterRequest, brokerAddrInfo);
                } finally {
                    this.lock.readLock().unlock();
                }
            } catch (Exception e) {
                log.error("onChannelDestroy Exception", e);
            }
        }

        if (needUnRegister) {
            boolean result = this.submitUnRegisterBrokerRequest(unRegisterRequest);
            log.info("the broker's channel destroyed, submit the unregister request at once, " +
                "broker info: {}, submit result: {}", unRegisterRequest, result);
        }
    }

    /**[]：当netty的通道被销毁(断开连接 或者 异常)的时候执行的回调。
     * 【方法的逻辑】：
     *      1.找到与该通道关联的 Broker 信息 。
     *      2.构造注销请求 （UnRegisterBrokerRequestHeader），并将其提交给 NameServer 的注销队列，以便从路由表
     *          中移除该 Broker 的相关信息(在this.routeInfoManager.start()中就会不断的执行从阻塞队列unregistrationQueue
     *          获取请求，去执行Broker的注销操作)。
     * @param channel：某一个通道
     * */
    public void onChannelDestroy(Channel channel) {
        UnRegisterBrokerRequestHeader unRegisterRequest = new UnRegisterBrokerRequestHeader();
        BrokerAddrInfo brokerAddrFound = null;
        boolean needUnRegister = false;
        if (channel != null) {
            try {
                try {
                    this.lock.readLock().lockInterruptibly();
                    for (Entry<BrokerAddrInfo, BrokerLiveInfo> entry : this.brokerLiveTable.entrySet()) {
                        if (entry.getValue().getChannel() == channel) {
                            brokerAddrFound = entry.getKey();
                            break;
                        }
                    }

                    if (brokerAddrFound != null) {
                        needUnRegister = setupUnRegisterRequest(unRegisterRequest, brokerAddrFound);
                    }
                } finally {
                    this.lock.readLock().unlock();
                }
            } catch (Exception e) {
                log.error("onChannelDestroy Exception", e);
            }
        }

        if (needUnRegister) {
            boolean result = this.submitUnRegisterBrokerRequest(unRegisterRequest);
            log.info("the broker's channel destroyed, submit the unregister request at once, " +
                "broker info: {}, submit result: {}", unRegisterRequest, result);
        }
    }

    private boolean setupUnRegisterRequest(UnRegisterBrokerRequestHeader unRegisterRequest,
        BrokerAddrInfo brokerAddrInfo) {
        unRegisterRequest.setClusterName(brokerAddrInfo.getClusterName());
        unRegisterRequest.setBrokerAddr(brokerAddrInfo.getBrokerAddr());

        for (Entry<String, BrokerData> stringBrokerDataEntry : this.brokerAddrTable.entrySet()) {
            BrokerData brokerData = stringBrokerDataEntry.getValue();
            if (!brokerAddrInfo.getClusterName().equals(brokerData.getCluster())) {
                continue;
            }

            for (Entry<Long, String> entry : brokerData.getBrokerAddrs().entrySet()) {
                Long brokerId = entry.getKey();
                String brokerAddr = entry.getValue();
                if (brokerAddr.equals(brokerAddrInfo.getBrokerAddr())) {
                    unRegisterRequest.setBrokerName(brokerData.getBrokerName());
                    unRegisterRequest.setBrokerId(brokerId);
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * 这段代码的功能是遍历需要通知的Broker状态变更信息，对于每个Broker，如果其启用了代理主节点功能，则调用notifyMinBrokerIdChanged方
     *      法进行最小Broker ID变更通知。
     * 具体步骤如下：
     *      遍历needNotifyBrokerMap中的每个Broker名称。
     *      获取该Broker的状态变更信息和对应的Broker数据。
     *      如果Broker数据不为空且启用了代理主节点功能，则调用notifyMinBrokerIdChanged方法，传递Broker地址、离线Broker地址和HA Broker地址。
     * */
    private void notifyMinBrokerIdChanged(Map<String, BrokerStatusChangeInfo> needNotifyBrokerMap)
        throws InterruptedException, RemotingConnectException, RemotingTimeoutException, RemotingSendRequestException,
        RemotingTooMuchRequestException {
        for (String brokerName : needNotifyBrokerMap.keySet()) {
            BrokerStatusChangeInfo brokerStatusChangeInfo = needNotifyBrokerMap.get(brokerName);
            BrokerData brokerData = brokerAddrTable.get(brokerName);
            if (brokerData != null && brokerData.isEnableActingMaster()) {
                notifyMinBrokerIdChanged(brokerStatusChangeInfo.getBrokerAddrs(),
                    brokerStatusChangeInfo.getOfflineBrokerAddr(), brokerStatusChangeInfo.getHaBrokerAddr());
            }
        }
    }

    private void notifyMinBrokerIdChanged(Map<Long, String> brokerAddrMap, String offlineBrokerAddr,
        String haBrokerAddr)
        throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException,
        RemotingTooMuchRequestException, RemotingConnectException {
        if (brokerAddrMap == null || brokerAddrMap.isEmpty() || this.namesrvController == null) {
            return;
        }

        NotifyMinBrokerIdChangeRequestHeader requestHeader = new NotifyMinBrokerIdChangeRequestHeader();
        long minBrokerId = Collections.min(brokerAddrMap.keySet());
        requestHeader.setMinBrokerId(minBrokerId);
        requestHeader.setMinBrokerAddr(brokerAddrMap.get(minBrokerId));
        requestHeader.setOfflineBrokerAddr(offlineBrokerAddr);
        requestHeader.setHaBrokerAddr(haBrokerAddr);

        List<String> brokerAddrsNotify = chooseBrokerAddrsToNotify(brokerAddrMap, offlineBrokerAddr);
        log.info("min broker id changed to {}, notify {}, offline broker addr {}", minBrokerId, brokerAddrsNotify, offlineBrokerAddr);
        RemotingCommand request =
            RemotingCommand.createRequestCommand(RequestCode.NOTIFY_MIN_BROKER_ID_CHANGE, requestHeader); //封装请求
        for (String brokerAddr : brokerAddrsNotify) { //
            this.namesrvController.getRemotingClient().invokeOneway(brokerAddr, request, 300); //基于底层的封装，这里就直接做到了请求的发送
        }
    }

    private List<String> chooseBrokerAddrsToNotify(Map<Long, String> brokerAddrMap, String offlineBrokerAddr) {
        if (offlineBrokerAddr != null || brokerAddrMap.size() == 1) {
            // notify the reset brokers.
            return new ArrayList<>(brokerAddrMap.values());
        }

        //否则将 除了最小的broker id 的其他所有的broker实例添加到列表返回
        // new broker registered, notify previous brokers.
        long minBrokerId = Collections.min(brokerAddrMap.keySet());
        List<String> brokerAddrList = new ArrayList<>();
        for (Long brokerId : brokerAddrMap.keySet()) {
            if (brokerId != minBrokerId) {
                brokerAddrList.add(brokerAddrMap.get(brokerId));
            }
        }
        return brokerAddrList;
    }

    // For test only
    public void printAllPeriodically() {
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                log.info("--------------------------------------------------------");
                {
                    log.info("topicQueueTable SIZE: {}", this.topicQueueTable.size());
                    for (Entry<String, Map<String, QueueData>> next : this.topicQueueTable.entrySet()) {
                        log.info("topicQueueTable Topic: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    log.info("brokerAddrTable SIZE: {}", this.brokerAddrTable.size());
                    for (Entry<String, BrokerData> next : this.brokerAddrTable.entrySet()) {
                        log.info("brokerAddrTable brokerName: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    log.info("brokerLiveTable SIZE: {}", this.brokerLiveTable.size());
                    for (Entry<BrokerAddrInfo, BrokerLiveInfo> next : this.brokerLiveTable.entrySet()) {
                        log.info("brokerLiveTable brokerAddr: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    log.info("clusterAddrTable SIZE: {}", this.clusterAddrTable.size());
                    for (Entry<String, Set<String>> next : this.clusterAddrTable.entrySet()) {
                        log.info("clusterAddrTable clusterName: {} {}", next.getKey(), next.getValue());
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("printAllPeriodically Exception", e);
        }
    }

    public TopicList getSystemTopicList() {
        TopicList topicList = new TopicList();
        try {
            this.lock.readLock().lockInterruptibly();
            for (Map.Entry<String, Set<String>> entry : clusterAddrTable.entrySet()) {
                topicList.getTopicList().add(entry.getKey());
                topicList.getTopicList().addAll(entry.getValue());
            }

            if (!brokerAddrTable.isEmpty()) {
                for (String s : brokerAddrTable.keySet()) {
                    BrokerData bd = brokerAddrTable.get(s);
                    HashMap<Long, String> brokerAddrs = bd.getBrokerAddrs();
                    if (brokerAddrs != null && !brokerAddrs.isEmpty()) {
                        Iterator<Long> it2 = brokerAddrs.keySet().iterator();
                        topicList.setBrokerAddr(brokerAddrs.get(it2.next()));
                        break;
                    }
                }
            }
        } catch (Exception e) {
            log.error("getSystemTopicList Exception", e);
        } finally {
            this.lock.readLock().unlock();
        }

        return topicList;
    }

    public TopicList getTopicsByCluster(String cluster) {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                Set<String> brokerNameSet = this.clusterAddrTable.get(cluster);
                for (String brokerName : brokerNameSet) {
                    for (Entry<String, Map<String, QueueData>> topicEntry : this.topicQueueTable.entrySet()) {
                        String topic = topicEntry.getKey();
                        Map<String, QueueData> queueDataMap = topicEntry.getValue();
                        final QueueData qd = queueDataMap.get(brokerName);
                        if (qd != null) {
                            topicList.getTopicList().add(topic);
                        }
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getTopicsByCluster Exception", e);
        }

        return topicList;
    }

    public TopicList getUnitTopics() {
        TopicList topicList = new TopicList();
        try {
            this.lock.readLock().lockInterruptibly();
            for (Entry<String, Map<String, QueueData>> topicEntry : this.topicQueueTable.entrySet()) {
                String topic = topicEntry.getKey();
                Map<String, QueueData> queueDatas = topicEntry.getValue();
                if (queueDatas != null && queueDatas.size() > 0
                    && TopicSysFlag.hasUnitFlag(queueDatas.values().iterator().next().getTopicSysFlag())) {
                    topicList.getTopicList().add(topic);
                }
            }
        } catch (Exception e) {
            log.error("getUnitTopics Exception", e);
        } finally {
            this.lock.readLock().unlock();
        }

        return topicList;
    }

    public TopicList getHasUnitSubTopicList() {
        TopicList topicList = new TopicList();
        try {
            this.lock.readLock().lockInterruptibly();
            for (Entry<String, Map<String, QueueData>> topicEntry : this.topicQueueTable.entrySet()) {
                String topic = topicEntry.getKey();
                Map<String, QueueData> queueDatas = topicEntry.getValue();
                if (queueDatas != null && queueDatas.size() > 0
                    && TopicSysFlag.hasUnitSubFlag(queueDatas.values().iterator().next().getTopicSysFlag())) {
                    topicList.getTopicList().add(topic);
                }
            }
        } catch (Exception e) {
            log.error("getHasUnitSubTopicList Exception", e);
        } finally {
            this.lock.readLock().unlock();
        }

        return topicList;
    }

    public TopicList getHasUnitSubUnUnitTopicList() {
        TopicList topicList = new TopicList();
        try {
            this.lock.readLock().lockInterruptibly();
            for (Entry<String, Map<String, QueueData>> topicEntry : this.topicQueueTable.entrySet()) {
                String topic = topicEntry.getKey();
                Map<String, QueueData> queueDatas = topicEntry.getValue();
                if (queueDatas != null && queueDatas.size() > 0
                    && !TopicSysFlag.hasUnitFlag(queueDatas.values().iterator().next().getTopicSysFlag())
                    && TopicSysFlag.hasUnitSubFlag(queueDatas.values().iterator().next().getTopicSysFlag())) {
                    topicList.getTopicList().add(topic);
                }
            }
        } catch (Exception e) {
            log.error("getHasUnitSubUnUnitTopicList Exception", e);
        } finally {
            this.lock.readLock().unlock();
        }

        return topicList;
    }
}

/**
 * broker address information
 */
class BrokerAddrInfo {
    private String clusterName;
    private String brokerAddr;

    private int hash;

    public BrokerAddrInfo(String clusterName, String brokerAddr) {
        this.clusterName = clusterName;
        this.brokerAddr = brokerAddr;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }

    public boolean isEmpty() {
        return clusterName.isEmpty() && brokerAddr.isEmpty();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }

        if (obj instanceof BrokerAddrInfo) {
            BrokerAddrInfo addr = (BrokerAddrInfo) obj;
            return clusterName.equals(addr.clusterName) && brokerAddr.equals(addr.brokerAddr);
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = hash;
        if (h == 0 && clusterName.length() + brokerAddr.length() > 0) {
            for (int i = 0; i < clusterName.length(); i++) {
                h = 31 * h + clusterName.charAt(i);
            }
            h = 31 * h + '_';
            for (int i = 0; i < brokerAddr.length(); i++) {
                h = 31 * h + brokerAddr.charAt(i);
            }
            hash = h;
        }
        return h;
    }

    @Override
    public String toString() {
        return "BrokerIdentityInfo [clusterName=" + clusterName + ", brokerAddr=" + brokerAddr + "]";
    }
}

/**作用：记录Broker的存活信息。更准确的说是根据Broker发来的心跳包，根据收到的心跳包组装一个BrokerLiveInfo对象来标记这个
 *      Broker的存活状态*/
class BrokerLiveInfo {
    private long lastUpdateTimestamp; //上次收到这个Broker发来心跳包的时间
    private long heartbeatTimeoutMillis;
    private DataVersion dataVersion;
    private Channel channel; //表示与该Broker的通信通道(即一个Channel对象)
    private String haServerAddr;

    public BrokerLiveInfo(long lastUpdateTimestamp, long heartbeatTimeoutMillis, DataVersion dataVersion,
        Channel channel,
        String haServerAddr) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
        this.heartbeatTimeoutMillis = heartbeatTimeoutMillis;
        this.dataVersion = dataVersion;
        this.channel = channel;
        this.haServerAddr = haServerAddr;
    }

    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }

    public long getHeartbeatTimeoutMillis() {
        return heartbeatTimeoutMillis;
    }

    public void setHeartbeatTimeoutMillis(long heartbeatTimeoutMillis) {
        this.heartbeatTimeoutMillis = heartbeatTimeoutMillis;
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(DataVersion dataVersion) {
        this.dataVersion = dataVersion;
    }

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    public String getHaServerAddr() {
        return haServerAddr;
    }

    public void setHaServerAddr(String haServerAddr) {
        this.haServerAddr = haServerAddr;
    }

    @Override
    public String toString() {
        return "BrokerLiveInfo [lastUpdateTimestamp=" + lastUpdateTimestamp + ", dataVersion=" + dataVersion
            + ", channel=" + channel + ", haServerAddr=" + haServerAddr + "]";
    }
}

class BrokerStatusChangeInfo {
    Map<Long, String> brokerAddrs;  // brokerId -> brokerAddr，表示broker集群中的各个broker实例的地址
    String offlineBrokerAddr;   // 是broker集群中已经下线的broker实例的地址
    String haBrokerAddr;    // broker集群中ha模式的broker实例的地址

    public BrokerStatusChangeInfo(Map<Long, String> brokerAddrs, String offlineBrokerAddr, String haBrokerAddr) {
        this.brokerAddrs = brokerAddrs;
        this.offlineBrokerAddr = offlineBrokerAddr;
        this.haBrokerAddr = haBrokerAddr;
    }

    public Map<Long, String> getBrokerAddrs() {
        return brokerAddrs;
    }

    public void setBrokerAddrs(Map<Long, String> brokerAddrs) {
        this.brokerAddrs = brokerAddrs;
    }

    public String getOfflineBrokerAddr() {
        return offlineBrokerAddr;
    }

    public void setOfflineBrokerAddr(String offlineBrokerAddr) {
        this.offlineBrokerAddr = offlineBrokerAddr;
    }

    public String getHaBrokerAddr() {
        return haBrokerAddr;
    }

    public void setHaBrokerAddr(String haBrokerAddr) {
        this.haBrokerAddr = haBrokerAddr;
    }
}
