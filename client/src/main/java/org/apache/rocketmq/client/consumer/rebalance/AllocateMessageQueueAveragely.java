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
package org.apache.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * Average Hashing queue algorithm
 */
public class AllocateMessageQueueAveragely extends AbstractAllocateMessageQueueStrategy {

    /**平均分配消息队列给消费者*/
    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
        List<String> cidAll) {

        List<MessageQueue> result = new ArrayList<>();
        if (!check(consumerGroup, currentCID, mqAll, cidAll)) { //检查参数，不符合要求时返回初始化的result
            return result;
        }

        int index = cidAll.indexOf(currentCID); //返回当前消费者在cidAll中的索引
        int mod = mqAll.size() % cidAll.size(); //消息队列总数 % 消费者数量，即计算出一个余数
        /*averageSize：表示平均一个消费者需要分配到的消息队列数量。。。。
        * mqAll.size() / cidAll.size()：表示平均一个消费者需要分配到几个消息队列
        * 整体计算averageSize的流程：
        *       如果mqAll.size() <= cidAll.size()：则每一个消费者平均分配一个队列(其实最真实的是有的消费者都分不到)；
        *       如果mqAll.size() > cidAll.size()，则需要分情况：
        *              情况①：index < mod，即当前消费者所在的索引 < 取余，这些消费者需要多分配一个消息队列
        *              情况②：index>=mod，后面的这些消费者就负责除数个消息队列就可以了
        * */
        int averageSize =
            mqAll.size() <= cidAll.size() ? 1 : (mod > 0 && index < mod ? mqAll.size() / cidAll.size()
                + 1 : mqAll.size() / cidAll.size());
        /*下面的所有逻辑，就是计算当前消费者clientId对应的消息队列集合，即result
        * 算法的模拟可以发现：是从前面开始，连续将n个消息队列分配某一个消费者(假设这个消费者应该被分配n个消息队列)
        *           比如：如果由15个消息队列，6个消费者，则index=0的消费者分配到的消息队列是0，1，2；index=1的
        *           消费者分配到的消息队列是3，4，5；index=2的消费者分配到的消息队列是6，7，8；index=3的消费者
        *           分配到的消息队列是9，10；index=4的消费者分配到的消息队列是11，12；index=5的消费者分配到的消
        *           息队列是13，14*/
        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        int range = Math.min(averageSize, mqAll.size() - startIndex);
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }
        return result;
    }

    @Override
    public String getName() {
        return "AVG";
    }
}
