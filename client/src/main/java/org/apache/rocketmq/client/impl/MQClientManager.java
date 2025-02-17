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
package org.apache.rocketmq.client.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.producer.ProduceAccumulator;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * 整个 JVM 实例中只存在一个MQClientManager实例。维护一个MQClientInstance缓存表ConcurrentMap<String,MQClientInstance> factoryTable
 *      即：同一个clientId只会创建一个MQClientInstance实例。
 * MQClientInstance是客户端各种类型的Consumer和Producer的底层类。这个类首先从NameServer获取并保存各种配置信息，比如
 *      Topic的Route信息。同时MQClientInstance还会通过MQClientAPIImpl类实现消息的收发，也就是从Broker获取消息或者发
 *      送消息到Broker。
 * 既然MQClientInstance实现的是底层通信功能和获取并保存元数据的功能，就没必要每个Consumer或Producer都创建一个对象，一
 *      个MQClientInstance对象可以被多个Consumer或Producer公用。
 * MQClientInstance封装了RocketMQ的网络处理API，是消息生产者、消息消费者与NameServer、Broker打交道的网络通道
 * */
public class MQClientManager {
    private final static Logger log = LoggerFactory.getLogger(MQClientManager.class);
    private static MQClientManager instance = new MQClientManager();
    private AtomicInteger factoryIndexGenerator = new AtomicInteger();  //后面在创建MQClientInstance实例的时候，会使用并自增该值。。最终会记录日志，除此以外没其他作用
    //整个JVM实例中只存在一个MQClientManager实例，维护一个MQClientInstance缓存表ConcurrentMap<String, MQClientInstance> factoryTable,即
    //同一个clientId只会创建一个MQClientInstance实例
    private ConcurrentMap<String/* clientId */, MQClientInstance> factoryTable =
        new ConcurrentHashMap<>();      //clientId的格式是“clientIp”+@+“InstanceName”
    private ConcurrentMap<String/* clientId */, ProduceAccumulator> accumulatorTable =
        new ConcurrentHashMap<String, ProduceAccumulator>();


    private MQClientManager() {

    }

    public static MQClientManager getInstance() {
        return instance;
    }

    public MQClientInstance getOrCreateMQClientInstance(final ClientConfig clientConfig) {
        return getOrCreateMQClientInstance(clientConfig, null);
    }
    /**
     * 1. 整个 JVM 实例中只存在一个MQClientManager实例，维护一个 MQClientlnstance 缓存表
     *      ConcurrentMap<String， MQClientinstance＞ factoryTable = new ConcurrentHashMap<String， MQClientlnstance＞（），
     *      也就是同一个 clientId 只会创建一个MQClientinstance。
     * 2. clientId为客户端IP+instance+unitname（可选），如果在同一台物理服务器部署两个应用程序，应用程序的clientId岂不是相同，这样是不是会造成混乱？
     *      为了避免出现这个问题，如果instance为默认值DEFAULT，RocketMQ会自动将instance设置为进程ID，这样就避免了不同进程相
            互影响，但同一个JVM中相同clientId的消费者和生产者在启动时获取的MQClientInstane实例都是同一个
     * */
    public MQClientInstance getOrCreateMQClientInstance(final ClientConfig clientConfig, RPCHook rpcHook) {
        String clientId = clientConfig.buildMQClientId();   // 根据客户端配置生成唯一的客户端ID
        MQClientInstance instance = this.factoryTable.get(clientId);     // 尝试从实例表中获取已存在的MQ客户端实例
        /**
         * 从下面的逻辑可以看出来，对于同样的clientId，MQClientInstance实例只会创建一个。
         * */
        if (null == instance) {     //如果实例不存在创建一个新的实例。
            instance =
                new MQClientInstance(clientConfig.cloneClientConfig(),
                    this.factoryIndexGenerator.getAndIncrement(), clientId, rpcHook);
            MQClientInstance prev = this.factoryTable.putIfAbsent(clientId, instance);
            if (prev != null) {
                instance = prev;
                log.warn("Returned Previous MQClientInstance for clientId:[{}]", clientId);
            } else {
                log.info("Created new MQClientInstance for clientId:[{}]", clientId);
            }
        }

        return instance;    //返回客户端实例
    }
    public ProduceAccumulator getOrCreateProduceAccumulator(final ClientConfig clientConfig) {
        String clientId = clientConfig.buildMQClientId();
        ProduceAccumulator accumulator = this.accumulatorTable.get(clientId);
        if (null == accumulator) {
            accumulator = new ProduceAccumulator(clientId);
            ProduceAccumulator prev = this.accumulatorTable.putIfAbsent(clientId, accumulator);
            if (prev != null) {
                accumulator = prev;
                log.warn("Returned Previous ProduceAccumulator for clientId:[{}]", clientId);
            } else {
                log.info("Created new ProduceAccumulator for clientId:[{}]", clientId);
            }
        }

        return accumulator;
    }

    public void removeClientFactory(final String clientId) {
        this.factoryTable.remove(clientId);
    }
}
