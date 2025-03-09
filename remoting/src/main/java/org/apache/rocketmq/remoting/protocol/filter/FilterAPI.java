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
package org.apache.rocketmq.remoting.protocol.filter;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

import java.util.Arrays;

public class FilterAPI {

    /*封装订阅主题所需数据(其实就是一些标志，比如:主题名、表达式)，返回封装结果SubscriptionData对象*/
    public static SubscriptionData buildSubscriptionData(String topic, String subString) throws Exception {
        final SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic(topic);
        subscriptionData.setSubString(subString);
        //subString为空 或者 subString为"*"，则将subString设置为"*"然后返回————表示订阅所有tag。即两种效果等同
        if (StringUtils.isEmpty(subString) || subString.equals(SubscriptionData.SUB_ALL)) {
            subscriptionData.setSubString(SubscriptionData.SUB_ALL);
            return subscriptionData;
        }
        /*将subString按照"||"进行分割得到tags数组。。比如"tag1||tag2||tag3"分割后tag变为长度为3的数组。
        * 并将多个tag放在当前组装数据的tagsSet属性中*/
        String[] tags = subString.split("\\|\\|");
        /*遍历tags数组，分别将 tag 和 tag的哈希值 放入subscriptionData的两个set中*/
        if (tags.length > 0) {
            Arrays.stream(tags).map(String::trim).filter(tag -> !tag.isEmpty()).forEach(tag -> {
                subscriptionData.getTagsSet().add(tag);
                subscriptionData.getCodeSet().add(tag.hashCode());
            });
        } else {
            throw new Exception("subString split error");
        }

        return subscriptionData;
    }

    public static SubscriptionData buildSubscriptionData(String topic, String subString, String expressionType) throws Exception {
        final SubscriptionData subscriptionData = buildSubscriptionData(topic, subString);
        if (StringUtils.isNotBlank(expressionType)) {
            subscriptionData.setExpressionType(expressionType);
        }
        return subscriptionData;
    }

    public static SubscriptionData build(final String topic, final String subString,
        final String type) throws Exception {
        if (ExpressionType.TAG.equals(type) || type == null) {
            return buildSubscriptionData(topic, subString);
        }

        if (StringUtils.isEmpty(subString)) {
            throw new IllegalArgumentException("Expression can't be null! " + type);
        }

        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic(topic);
        subscriptionData.setSubString(subString);
        subscriptionData.setExpressionType(type);

        return subscriptionData;
    }
}
