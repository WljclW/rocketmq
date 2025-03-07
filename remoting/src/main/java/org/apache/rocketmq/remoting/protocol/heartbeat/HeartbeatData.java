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

/**
 * $Id: HeartbeatData.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.remoting.protocol.heartbeat;

import java.util.HashSet;
import java.util.Set;
import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class HeartbeatData extends RemotingSerializable {
    private String clientID;
    private Set<ProducerData> producerDataSet = new HashSet<>();
    private Set<ConsumerData> consumerDataSet = new HashSet<>();
    private int heartbeatFingerprint = 0;
    private boolean isWithoutSub = false; //标识消费者 是否没有订阅任何主题

    public String getClientID() {
        return clientID;
    }

    public void setClientID(String clientID) {
        this.clientID = clientID;
    }

    public Set<ProducerData> getProducerDataSet() {
        return producerDataSet;
    }

    public void setProducerDataSet(Set<ProducerData> producerDataSet) {
        this.producerDataSet = producerDataSet;
    }

    public Set<ConsumerData> getConsumerDataSet() {
        return consumerDataSet;
    }

    public void setConsumerDataSet(Set<ConsumerData> consumerDataSet) {
        this.consumerDataSet = consumerDataSet;
    }

    public int getHeartbeatFingerprint() {
        return heartbeatFingerprint;
    }

    public void setHeartbeatFingerprint(int heartbeatFingerprint) {
        this.heartbeatFingerprint = heartbeatFingerprint;
    }

    public boolean isWithoutSub() {
        return isWithoutSub;
    }

    public void setWithoutSub(boolean withoutSub) {
        isWithoutSub = withoutSub;
    }

    @Override
    public String toString() {
        return "HeartbeatData [clientID=" + clientID + ", producerDataSet=" + producerDataSet
            + ", consumerDataSet=" + consumerDataSet + "]";
    }

    /**计算一个心跳包HeartbeatData的指纹(其实就是计算哈希值)，深拷贝得到一个新对象后置无关字段(换言之关注心跳包的实质性内容)为零值，然后
     * 序列化成字符串并计算它的哈希值*/
    public int computeHeartbeatFingerprint() {
        HeartbeatData heartbeatDataCopy = JSON.parseObject(JSON.toJSONString(this), HeartbeatData.class);
        /*下面的几行是将无关字段置0，比如：订阅数据版本号置0、包含订阅关系、置指纹值为0、置clientId为0。。
        * 为什么需要这样做？？
        *       猜测：这些字段的值可能会因为环境改变而改变，但是重要的是他们不影响心跳检测的实际内容，因此他们置为相同的可以让
        *       这里的哈希值集中关注实质性的内容*/
        for (ConsumerData consumerData : heartbeatDataCopy.getConsumerDataSet()) {
            for (SubscriptionData subscriptionData : consumerData.getSubscriptionDataSet()) {
                subscriptionData.setSubVersion(0L);
            }
        }
        heartbeatDataCopy.setWithoutSub(false);
        heartbeatDataCopy.setHeartbeatFingerprint(0);
        heartbeatDataCopy.setClientID("");
        /*将heartbeatDataCopy对象重新序列化成json串并计算它的哈希值，返回*/
        return JSON.toJSONString(heartbeatDataCopy).hashCode();
    }
}
