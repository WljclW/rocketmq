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

package org.apache.rocketmq.broker.filter;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.filter.util.BitsArray;
import org.apache.rocketmq.filter.util.BloomFilter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.MessageFilter;

public class ExpressionMessageFilter implements MessageFilter {

    protected static final Logger log = LoggerFactory.getLogger(LoggerName.FILTER_LOGGER_NAME);

    protected final SubscriptionData subscriptionData;
    protected final ConsumerFilterData consumerFilterData;
    protected final ConsumerFilterManager consumerFilterManager;
    protected final boolean bloomDataValid;

    public ExpressionMessageFilter(SubscriptionData subscriptionData, ConsumerFilterData consumerFilterData,
        ConsumerFilterManager consumerFilterManager) {
        this.subscriptionData = subscriptionData;
        this.consumerFilterData = consumerFilterData;
        this.consumerFilterManager = consumerFilterManager;
        if (consumerFilterData == null) {
            bloomDataValid = false;
            return;
        }
        BloomFilter bloomFilter = this.consumerFilterManager.getBloomFilter();
        if (bloomFilter != null && bloomFilter.isValid(consumerFilterData.getBloomFilterData())) {
            bloomDataValid = true;
        } else {
            bloomDataValid = false;
        }
    }

    /**用于在 Broker 端 基于 ConsumeQueue 的数据对消息进行初步过滤。它的主要目的是在拉取消息时，快速判断
     * 某条消息是否符合消费者的订阅条件*/
    @Override
    public boolean isMatchedByConsumeQueue(Long tagsCode, ConsumeQueueExt.CqExtUnit cqExtUnit) {
        //如果订阅数据是null，则表示订阅数据为空，则表示订阅所有消息（如果 subscriptionData 为 null，表示消费者没有指定任
        //何订阅条件，默认订阅所有消息，返回 true。）————则返回true.
        if (null == subscriptionData) {
            return true;
        }
        //如果订阅数据是classFilterMode，则表示订阅所有消息，则返回true
        if (subscriptionData.isClassFilterMode()) {
            return true;
        }
        /**根据是否TAG类型进行分类处理：
         * if块：处理TAG类型的过滤
         * else：处理其他的情况*/
        // by tags code.如果是‘表达式过滤’中的TAG类型的消息过滤，使用下面的if块
        if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {
            //没有哈希码 或者 订阅表达式是"*"，表示订阅所有，返回true
            if (tagsCode == null) {
                return true;
            }
            if (subscriptionData.getSubString().equals(SubscriptionData.SUB_ALL)) {
                return true;
            }
            /* 比对tagsCode是不是包含在订阅信息的tagsSet中，如果包含，则返回true，否则返回false————完成对tag表达式
                    的'初步'过滤(仅仅过滤了哈希码)
            【注意】基于TAG模式根据ConsumeQueue进行消息过滤时只对比tag的哈希码，所以还需要
            在消息消费端对消息标志进行精确匹配*/
            return subscriptionData.getCodeSet().contains(tagsCode.intValue());
        } else { /**只要不是TAG类型的过滤类型，都走else分支*/
            // no expression or no bloom....没有表达式 或者 没有布隆过滤器，则返回true
            if (consumerFilterData == null || consumerFilterData.getExpression() == null
                || consumerFilterData.getCompiledExpression() == null || consumerFilterData.getBloomFilterData() == null) {
                return true;
            }

            // message is before consumer————下面的代码表示消费者订阅的时候，消息还没有存储
            if (cqExtUnit == null || !consumerFilterData.isMsgInLive(cqExtUnit.getMsgStoreTime())) {
                log.debug("Pull matched because not in live: {}, {}", consumerFilterData, cqExtUnit);
                return true;
            }
            /*下面的逻辑是 布隆过滤器校验 的逻辑*/
            byte[] filterBitMap = cqExtUnit.getFilterBitMap();
            BloomFilter bloomFilter = this.consumerFilterManager.getBloomFilter();
            if (filterBitMap == null || !this.bloomDataValid
                || filterBitMap.length * Byte.SIZE != consumerFilterData.getBloomFilterData().getBitNum()) {
                return true;
            }

            BitsArray bitsArray = null;
            try {
                bitsArray = BitsArray.create(filterBitMap);
                boolean ret = bloomFilter.isHit(consumerFilterData.getBloomFilterData(), bitsArray);
                log.debug("Pull {} by bit map:{}, {}, {}", ret, consumerFilterData, bitsArray, cqExtUnit);
                return ret;
            } catch (Throwable e) {
                log.error("bloom filter error, sub=" + subscriptionData
                    + ", filter=" + consumerFilterData + ", bitMap=" + bitsArray, e);
            }
        }

        return true;
    }

    @Override
    public boolean isMatchedByCommitLog(ByteBuffer msgBuffer, Map<String, String> properties) {
        if (subscriptionData == null) {
            return true;
        }

        if (subscriptionData.isClassFilterMode()) {
            return true;
        }

        if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {
            return true;
        }

        ConsumerFilterData realFilterData = this.consumerFilterData;
        Map<String, String> tempProperties = properties;

        // no expression
        if (realFilterData == null || realFilterData.getExpression() == null
            || realFilterData.getCompiledExpression() == null) {
            return true;
        }

        if (tempProperties == null && msgBuffer != null) {
            tempProperties = MessageDecoder.decodeProperties(msgBuffer);
        }

        Object ret = null;
        try {
            MessageEvaluationContext context = new MessageEvaluationContext(tempProperties);

            ret = realFilterData.getCompiledExpression().evaluate(context);
        } catch (Throwable e) {
            log.error("Message Filter error, " + realFilterData + ", " + tempProperties, e);
        }

        log.debug("Pull eval result: {}, {}, {}", ret, realFilterData, tempProperties);

        if (ret == null || !(ret instanceof Boolean)) {
            return false;
        }

        return (Boolean) ret;
    }

}
