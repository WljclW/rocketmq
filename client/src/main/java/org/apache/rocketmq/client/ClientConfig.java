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
package org.apache.rocketmq.client;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.utils.NameServerAddressUtils;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.remoting.protocol.RequestType;

/**
 * Client Common configuration....
 * 配置客户端的通用配置，内部提供默认的消费者 和 生产者 都是它的子类.
 * 每一个该类或其子类的对象都会根据buildMQClientId方法构建clientid————区分不同实例的根据
 */
public class ClientConfig {
    public static final String SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY = "com.rocketmq.sendMessageWithVIPChannel";
    public static final String SOCKS_PROXY_CONFIG = "com.rocketmq.socks.proxy.config";
    public static final String DECODE_READ_BODY = "com.rocketmq.read.body";
    public static final String DECODE_DECOMPRESS_BODY = "com.rocketmq.decompress.body";
    public static final String SEND_LATENCY_ENABLE = "com.rocketmq.sendLatencyEnable";
    public static final String START_DETECTOR_ENABLE = "com.rocketmq.startDetectorEnable";
    public static final String HEART_BEAT_V2 = "com.rocketmq.heartbeat.v2";
    private String namesrvAddr = NameServerAddressUtils.getNameServerAddresses();
    private String clientIP = NetworkUtil.getLocalAddress();
    /*构建clientid的时候使用..
    在方法DefaultMQProducerImpl.start(boolean)中，会调用到this.defaultMQProducer.changeInstanceNameToPID()————
        此时如果这个属性值是”DEFAULT“，就会将这个值重新设置为”pid+纳秒数“*/
    private String instanceName = System.getProperty("rocketmq.client.name", "DEFAULT");
    private int clientCallbackExecutorThreads = Runtime.getRuntime().availableProcessors();
    @Deprecated
    protected String namespace;
    private boolean namespaceInitialized = false;
    protected String namespaceV2;
    protected AccessChannel accessChannel = AccessChannel.LOCAL;

    /**
     * Pulling topic information interval from the named server
     */
    private int pollNameServerInterval = 1000 * 30;
    /**
     * Heartbeat interval in microseconds with message broker
     */
    private int heartbeatBrokerInterval = 1000 * 30;
    /**
     * Offset persistent interval for consumer
     */
    private int persistConsumerOffsetInterval = 1000 * 5;
    private long pullTimeDelayMillsWhenException = 1000;
    private boolean unitMode = false;
    private String unitName;  //自定义字段(构建clientid的时候会用到)，作用：在构建客户端 ID 时进一步区分不同的逻辑单元或业务单元。有助于在多租户或分布式环境中更精确地管理和识别客户端。
    private boolean decodeReadBody = Boolean.parseBoolean(System.getProperty(DECODE_READ_BODY, "true"));
    private boolean decodeDecompressBody = Boolean.parseBoolean(System.getProperty(DECODE_DECOMPRESS_BODY, "true"));
    private boolean vipChannelEnabled = Boolean.parseBoolean(System.getProperty(SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY, "false"));
    private boolean useHeartbeatV2 = Boolean.parseBoolean(System.getProperty(HEART_BEAT_V2, "false"));

    private boolean useTLS = TlsSystemConfig.tlsEnable;

    private String socksProxyConfig = System.getProperty(SOCKS_PROXY_CONFIG, "{}");

    private int mqClientApiTimeout = 3 * 1000;
    private int detectTimeout = 200;
    private int detectInterval = 2 * 1000;

    private LanguageCode language = LanguageCode.JAVA;

    /**
     * Enable stream request type will inject a RPCHook to add corresponding request type to remoting layer.
     * And it will also generate a different client id to prevent unexpected reuses of MQClientInstance.
     */
    protected boolean enableStreamRequestType = false;

    /**
     * Enable the fault tolerance mechanism of the client sending process.
     * DO NOT OPEN when ORDER messages are required.
     * Turning on will interfere with the queue selection functionality,
     * possibly conflicting with the order message.
     */
    private boolean sendLatencyEnable = Boolean.parseBoolean(System.getProperty(SEND_LATENCY_ENABLE, "false"));
    private boolean startDetectorEnable = Boolean.parseBoolean(System.getProperty(START_DETECTOR_ENABLE, "false"));

    /*enableHeartbeatChannelEventListener决定是否在客户端中注册一个专门的事件监听器，用于监听
    与 Broker 的心跳通道相关的事件。
    主要用于增强客户端与 Broker 之间的连接管理能力，特别是在网络不稳定或 Broker 故障的情况下。*/
    private boolean enableHeartbeatChannelEventListener = true; /*是否启用 心跳事件通道监听器*/

    /**根据client的ip、instanceName、unitName构建mqClientId..完整的名称：IP地址@InstanceName@unitName@0。。
     * 【说明】
     *      1. 单纯这么看，如果同一个机器的不同生产者，是可能重名的。
     *          其实并不会。生产者启动时必须会调用到DefaultMQProducerImpl#start(boolean)，在这个方法中有一步
     *          表明：如果生产者组不是”MixAll.CLIENT_INNER_PRODUCER_GROUP“，就会修改instanceName属性*/
    public String buildMQClientId() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClientIP());

        sb.append("@");
        sb.append(this.getInstanceName());
        if (!UtilAll.isBlank(this.unitName)) {  //unitName不是空则拼接
            sb.append("@");
            sb.append(this.unitName);
        }

        if (enableStreamRequestType) {
            sb.append("@");
            sb.append(RequestType.STREAM);
        }

        return sb.toString();
    }

    public String getClientIP() {
        return clientIP;
    }

    public void setClientIP(String clientIP) {
        this.clientIP = clientIP;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    /**[]：修改实例名为PID(jps命令可查) +"#"+ 当前的纳秒值*/
    public void changeInstanceNameToPID() {
        if (this.instanceName.equals("DEFAULT")) {
            this.instanceName = UtilAll.getPid() + "#" + System.nanoTime();
        }
    }

    @Deprecated
    public String withNamespace(String resource) {
        return NamespaceUtil.wrapNamespace(this.getNamespace(), resource);
    }

    @Deprecated
    public Set<String> withNamespace(Set<String> resourceSet) {
        Set<String> resourceWithNamespace = new HashSet<>();
        for (String resource : resourceSet) {
            resourceWithNamespace.add(withNamespace(resource));
        }
        return resourceWithNamespace;
    }

    @Deprecated
    public String withoutNamespace(String resource) {
        return NamespaceUtil.withoutNamespace(resource, this.getNamespace());
    }

    @Deprecated
    public Set<String> withoutNamespace(Set<String> resourceSet) {
        Set<String> resourceWithoutNamespace = new HashSet<>();
        for (String resource : resourceSet) {
            resourceWithoutNamespace.add(withoutNamespace(resource));
        }
        return resourceWithoutNamespace;
    }

    @Deprecated
    public MessageQueue queueWithNamespace(MessageQueue queue) {
        if (StringUtils.isEmpty(this.getNamespace())) {
            return queue;
        }
        return new MessageQueue(withNamespace(queue.getTopic()), queue.getBrokerName(), queue.getQueueId());
    }

    @Deprecated
    public Collection<MessageQueue> queuesWithNamespace(Collection<MessageQueue> queues) {
        if (StringUtils.isEmpty(this.getNamespace())) {
            return queues;
        }
        Iterator<MessageQueue> iter = queues.iterator();
        while (iter.hasNext()) {
            MessageQueue queue = iter.next();
            queue.setTopic(withNamespace(queue.getTopic()));
        }
        return queues;
    }

    public void resetClientConfig(final ClientConfig cc) {
        this.namesrvAddr = cc.namesrvAddr;
        this.clientIP = cc.clientIP;
        this.instanceName = cc.instanceName;
        this.clientCallbackExecutorThreads = cc.clientCallbackExecutorThreads;
        this.pollNameServerInterval = cc.pollNameServerInterval;
        this.heartbeatBrokerInterval = cc.heartbeatBrokerInterval;
        this.persistConsumerOffsetInterval = cc.persistConsumerOffsetInterval;
        this.pullTimeDelayMillsWhenException = cc.pullTimeDelayMillsWhenException;
        this.unitMode = cc.unitMode;
        this.unitName = cc.unitName;
        this.vipChannelEnabled = cc.vipChannelEnabled;
        this.useTLS = cc.useTLS;
        this.socksProxyConfig = cc.socksProxyConfig;
        this.namespace = cc.namespace;
        this.language = cc.language;
        this.mqClientApiTimeout = cc.mqClientApiTimeout;
        this.decodeReadBody = cc.decodeReadBody;
        this.decodeDecompressBody = cc.decodeDecompressBody;
        this.enableStreamRequestType = cc.enableStreamRequestType;
        this.useHeartbeatV2 = cc.useHeartbeatV2;
        this.startDetectorEnable = cc.startDetectorEnable;
        this.sendLatencyEnable = cc.sendLatencyEnable;
        this.enableHeartbeatChannelEventListener = cc.enableHeartbeatChannelEventListener;
        this.detectInterval = cc.detectInterval;
        this.detectTimeout = cc.detectTimeout;
        this.namespaceV2 = cc.namespaceV2;
    }

    public ClientConfig cloneClientConfig() {
        ClientConfig cc = new ClientConfig();
        cc.namesrvAddr = namesrvAddr;
        cc.clientIP = clientIP;
        cc.instanceName = instanceName;
        cc.clientCallbackExecutorThreads = clientCallbackExecutorThreads;
        cc.pollNameServerInterval = pollNameServerInterval;
        cc.heartbeatBrokerInterval = heartbeatBrokerInterval;
        cc.persistConsumerOffsetInterval = persistConsumerOffsetInterval;
        cc.pullTimeDelayMillsWhenException = pullTimeDelayMillsWhenException;
        cc.unitMode = unitMode;
        cc.unitName = unitName;
        cc.vipChannelEnabled = vipChannelEnabled;
        cc.useTLS = useTLS;
        cc.socksProxyConfig = socksProxyConfig;
        cc.namespace = namespace;
        cc.language = language;
        cc.mqClientApiTimeout = mqClientApiTimeout;
        cc.decodeReadBody = decodeReadBody;
        cc.decodeDecompressBody = decodeDecompressBody;
        cc.enableStreamRequestType = enableStreamRequestType;
        cc.useHeartbeatV2 = useHeartbeatV2;
        cc.startDetectorEnable = startDetectorEnable;
        cc.enableHeartbeatChannelEventListener = enableHeartbeatChannelEventListener;
        cc.sendLatencyEnable = sendLatencyEnable;
        cc.detectInterval = detectInterval;
        cc.detectTimeout = detectTimeout;
        cc.namespaceV2 = namespaceV2;
        return cc;
    }

    /**拿到用户配置的namesrvAddr，返回endpoint这样的形式，即不包括”http://“*/
    public String getNamesrvAddr() {
        /*如果是http开始这样的形式，就进if语句块拿到http://之后的字符串*/
        if (StringUtils.isNotEmpty(namesrvAddr) && NameServerAddressUtils.NAMESRV_ENDPOINT_PATTERN.matcher(namesrvAddr.trim()).matches()) {
            return NameServerAddressUtils.getNameSrvAddrFromNamesrvEndpoint(namesrvAddr);
        }
        /*否则直接返回*/
        return namesrvAddr;
    }

    /**
     * Domain name mode access way does not support the delimiter(;), and only one domain name can be set.
     *
     * @param namesrvAddr name server address
     */
    public void setNamesrvAddr(String namesrvAddr) { /*这里的参数只能携带一个namesrv*/
        this.namesrvAddr = namesrvAddr;
        this.namespaceInitialized = false;
    }

    public int getClientCallbackExecutorThreads() {
        return clientCallbackExecutorThreads;
    }

    public void setClientCallbackExecutorThreads(int clientCallbackExecutorThreads) {
        this.clientCallbackExecutorThreads = clientCallbackExecutorThreads;
    }

    public int getPollNameServerInterval() {
        return pollNameServerInterval;
    }

    public void setPollNameServerInterval(int pollNameServerInterval) {
        this.pollNameServerInterval = pollNameServerInterval;
    }

    public int getHeartbeatBrokerInterval() {
        return heartbeatBrokerInterval;
    }

    public void setHeartbeatBrokerInterval(int heartbeatBrokerInterval) {
        this.heartbeatBrokerInterval = heartbeatBrokerInterval;
    }

    public int getPersistConsumerOffsetInterval() {
        return persistConsumerOffsetInterval;
    }

    public void setPersistConsumerOffsetInterval(int persistConsumerOffsetInterval) {
        this.persistConsumerOffsetInterval = persistConsumerOffsetInterval;
    }

    public long getPullTimeDelayMillsWhenException() {
        return pullTimeDelayMillsWhenException;
    }

    public void setPullTimeDelayMillsWhenException(long pullTimeDelayMillsWhenException) {
        this.pullTimeDelayMillsWhenException = pullTimeDelayMillsWhenException;
    }

    public String getUnitName() {
        return unitName;
    }

    public void setUnitName(String unitName) {
        this.unitName = unitName;
    }

    public boolean isUnitMode() {
        return unitMode;
    }

    public void setUnitMode(boolean unitMode) {
        this.unitMode = unitMode;
    }

    public boolean isVipChannelEnabled() {
        return vipChannelEnabled;
    }

    public void setVipChannelEnabled(final boolean vipChannelEnabled) {
        this.vipChannelEnabled = vipChannelEnabled;
    }

    public boolean isUseTLS() {
        return useTLS;
    }

    public void setUseTLS(boolean useTLS) {
        this.useTLS = useTLS;
    }

    public String getSocksProxyConfig() {
        return socksProxyConfig;
    }

    public void setSocksProxyConfig(String socksProxyConfig) {
        this.socksProxyConfig = socksProxyConfig;
    }

    public LanguageCode getLanguage() {
        return language;
    }

    public void setLanguage(LanguageCode language) {
        this.language = language;
    }

    public boolean isDecodeReadBody() {
        return decodeReadBody;
    }

    public void setDecodeReadBody(boolean decodeReadBody) {
        this.decodeReadBody = decodeReadBody;
    }

    public boolean isDecodeDecompressBody() {
        return decodeDecompressBody;
    }

    public void setDecodeDecompressBody(boolean decodeDecompressBody) {
        this.decodeDecompressBody = decodeDecompressBody;
    }

    /**
     * []：获取命名空间的方法
     * 主要用于实现 多租户隔离 和 资源逻辑分组 。它的作用类似于其他分布式系统中的命名空间或租户标识，用于在同
     *      一套 RocketMQ 集群中支持多个独立的逻辑分区，允许不同的租户在同一个集群中隔离资源（如 Topic 和
     *      Group）。。指定nameSpace之后，不同nameSpace下可以用相同的topic等资源
     * */
    @Deprecated
    public String getNamespace() {
        if (namespaceInitialized) {
            return namespace;
        }

        if (StringUtils.isNotEmpty(namespace)) {
            return namespace;
        }

        if (StringUtils.isNotEmpty(this.namesrvAddr)) {
            if (NameServerAddressUtils.validateInstanceEndpoint(namesrvAddr)) {
                namespace = NameServerAddressUtils.parseInstanceIdFromEndpoint(namesrvAddr);
            }
        }
        namespaceInitialized = true;
        return namespace;
    }

    @Deprecated
    public void setNamespace(String namespace) {
        this.namespace = namespace;
        this.namespaceInitialized = true;
    }

    public String getNamespaceV2() {
        return namespaceV2;
    }

    public void setNamespaceV2(String namespaceV2) {
        this.namespaceV2 = namespaceV2;
    }

    public AccessChannel getAccessChannel() {
        return this.accessChannel;
    }

    public void setAccessChannel(AccessChannel accessChannel) {
        this.accessChannel = accessChannel;
    }

    public int getMqClientApiTimeout() {
        return mqClientApiTimeout;
    }

    public void setMqClientApiTimeout(int mqClientApiTimeout) {
        this.mqClientApiTimeout = mqClientApiTimeout;
    }

    public boolean isEnableStreamRequestType() {
        return enableStreamRequestType;
    }

    public void setEnableStreamRequestType(boolean enableStreamRequestType) {
        this.enableStreamRequestType = enableStreamRequestType;
    }

    public boolean isSendLatencyEnable() {
        return sendLatencyEnable;
    }

    public void setSendLatencyEnable(boolean sendLatencyEnable) {
        this.sendLatencyEnable = sendLatencyEnable;
    }

    public boolean isStartDetectorEnable() {
        return startDetectorEnable;
    }

    public void setStartDetectorEnable(boolean startDetectorEnable) {
        this.startDetectorEnable = startDetectorEnable;
    }

    public boolean isEnableHeartbeatChannelEventListener() {
        return enableHeartbeatChannelEventListener;
    }

    public void setEnableHeartbeatChannelEventListener(boolean enableHeartbeatChannelEventListener) {
        this.enableHeartbeatChannelEventListener = enableHeartbeatChannelEventListener;
    }

    public int getDetectTimeout() {
        return this.detectTimeout;
    }

    public void setDetectTimeout(int detectTimeout) {
        this.detectTimeout = detectTimeout;
    }

    public int getDetectInterval() {
        return this.detectInterval;
    }

    public void setDetectInterval(int detectInterval) {
        this.detectInterval = detectInterval;
    }

    public boolean isUseHeartbeatV2() {
        return useHeartbeatV2;
    }

    public void setUseHeartbeatV2(boolean useHeartbeatV2) {
        this.useHeartbeatV2 = useHeartbeatV2;
    }

    @Override
    public String toString() {
        return "ClientConfig{" +
            "namesrvAddr='" + namesrvAddr + '\'' +
            ", clientIP='" + clientIP + '\'' +
            ", instanceName='" + instanceName + '\'' +
            ", clientCallbackExecutorThreads=" + clientCallbackExecutorThreads +
            ", namespace='" + namespace + '\'' +
            ", namespaceInitialized=" + namespaceInitialized +
            ", namespaceV2='" + namespaceV2 + '\'' +
            ", accessChannel=" + accessChannel +
            ", pollNameServerInterval=" + pollNameServerInterval +
            ", heartbeatBrokerInterval=" + heartbeatBrokerInterval +
            ", persistConsumerOffsetInterval=" + persistConsumerOffsetInterval +
            ", pullTimeDelayMillsWhenException=" + pullTimeDelayMillsWhenException +
            ", unitMode=" + unitMode +
            ", unitName='" + unitName + '\'' +
            ", decodeReadBody=" + decodeReadBody +
            ", decodeDecompressBody=" + decodeDecompressBody +
            ", vipChannelEnabled=" + vipChannelEnabled +
            ", useHeartbeatV2=" + useHeartbeatV2 +
            ", useTLS=" + useTLS +
            ", socksProxyConfig='" + socksProxyConfig + '\'' +
            ", mqClientApiTimeout=" + mqClientApiTimeout +
            ", detectTimeout=" + detectTimeout +
            ", detectInterval=" + detectInterval +
            ", language=" + language +
            ", enableStreamRequestType=" + enableStreamRequestType +
            ", sendLatencyEnable=" + sendLatencyEnable +
            ", startDetectorEnable=" + startDetectorEnable +
            ", enableHeartbeatChannelEventListener=" + enableHeartbeatChannelEventListener +
            '}';
    }
}
