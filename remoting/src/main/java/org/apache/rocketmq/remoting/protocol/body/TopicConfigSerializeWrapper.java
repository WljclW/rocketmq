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

package org.apache.rocketmq.remoting.protocol.body;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * 在 RocketMQ 中：
         TopicConfig 表示一个 Topic 的配置（如读写队列数、权限、是否顺序消息等）;所有 Topic 的配置保存
    在 TopicConfigManager.topicConfigTable（内存中）。当 Broker 启动或配置变更时，需要将这些配置持久
    化到磁盘（topicConfig.json）。当 Broker 重启时，需要从磁盘文件中恢复这些配置
      但问题来了：
          ❌ TopicConfigManager 本身是一个复杂的管理器，包含线程池、监听器、网络组件等，不能直接序列化！
          ✅ 所以引入 TopicConfigSerializeWrapper —— 只包装“可持久化”的那部分数据。
 * 【补充说明】 除了这个类以外，在方法”BrokerController#initializeMetadata“中多个类加载时decode的时候，都是
 *      使用了类似的方法————创建XxxxWrapper类，专注于持久化的字段
 */
public class TopicConfigSerializeWrapper extends RemotingSerializable {
    private ConcurrentMap<String, TopicConfig> topicConfigTable =
        new ConcurrentHashMap<>();
    private DataVersion dataVersion = new DataVersion();

    public ConcurrentMap<String, TopicConfig> getTopicConfigTable() {
        return topicConfigTable;
    }

    public void setTopicConfigTable(ConcurrentMap<String, TopicConfig> topicConfigTable) {
        this.topicConfigTable = topicConfigTable;
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(DataVersion dataVersion) {
        this.dataVersion = dataVersion;
    }
}
