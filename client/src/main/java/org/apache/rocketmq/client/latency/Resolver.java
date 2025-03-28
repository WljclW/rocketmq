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

/**
 * Resolver接口的作用本质上应该表述为：给出一个BrokerName，解析出一个Broker的地址。具体的实现逻辑
 *      则由方法resolve的编码实现。比如：在DefaultMQProducerImpl的构造方法中，创建MQFaultStrategy
 *      对象时，会创建一个匿名的Resolver对象，它的resolve方法的逻辑是返回这个Broker集群中id=0的节点地址
 * */
public interface Resolver {

    String resolve(String name /*brokerName*/);
}
