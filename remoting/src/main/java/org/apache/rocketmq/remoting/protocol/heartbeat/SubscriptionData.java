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
 * $Id: SubscriptionData.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.remoting.protocol.heartbeat;

import com.alibaba.fastjson.annotation.JSONField;
import java.util.HashSet;
import java.util.Set;
import org.apache.rocketmq.common.filter.ExpressionType;

/**
 * 【总述】作用是保存消费者订阅的主题信息、标签信息和其它相关的订阅参数。消费者通过该类标识 自己感兴趣的主题 和 消息过滤
 *      规则。RocketMQ在运行时根据SubscriptionData来决定向哪个消费者推送哪些消息。
 * 【其他】
 * 1. 重要！！对于rocketmq使用者来说，消费时指定topic、tag等；但是在内部，区分消费者订阅的队列依靠的是SubscriptionData对象(用这个类
 *      的对象来标识当前消费者需要消费哪些消息)
 * */
public class SubscriptionData implements Comparable<SubscriptionData> {
    public final static String SUB_ALL = "*"; //表示订阅所有消息，默认订阅所有
    private boolean classFilterMode = false; //是否使用类过滤器
    private String topic; //主题名称
    private String subString; //消息过滤表达式，多个用双竖线隔开，例如“TAGA|| TAGB”。
    private Set<String> tagsSet = new HashSet<>(); //标签集合
    private Set<Integer> codeSet = new HashSet<>(); //消息过滤标志哈希码集合
    private long subVersion = System.currentTimeMillis(); //订阅版本号。默认为当前时间戳
    private String expressionType = ExpressionType.TAG; //消息过滤表达式类型

    @JSONField(serialize = false)
    private String filterClassSource;

    public SubscriptionData() {

    }

    public SubscriptionData(String topic, String subString) {
        super();
        this.topic = topic;
        this.subString = subString;
    }

    public String getFilterClassSource() {
        return filterClassSource;
    }

    public void setFilterClassSource(String filterClassSource) {
        this.filterClassSource = filterClassSource;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getSubString() {
        return subString;
    }

    public void setSubString(String subString) {
        this.subString = subString;
    }

    public Set<String> getTagsSet() {
        return tagsSet;
    }

    public void setTagsSet(Set<String> tagsSet) {
        this.tagsSet = tagsSet;
    }

    public long getSubVersion() {
        return subVersion;
    }

    public void setSubVersion(long subVersion) {
        this.subVersion = subVersion;
    }

    public Set<Integer> getCodeSet() {
        return codeSet;
    }

    public void setCodeSet(Set<Integer> codeSet) {
        this.codeSet = codeSet;
    }

    public boolean isClassFilterMode() {
        return classFilterMode;
    }

    public void setClassFilterMode(boolean classFilterMode) {
        this.classFilterMode = classFilterMode;
    }

    public String getExpressionType() {
        return expressionType;
    }

    public void setExpressionType(String expressionType) {
        this.expressionType = expressionType;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (classFilterMode ? 1231 : 1237);
        result = prime * result + ((codeSet == null) ? 0 : codeSet.hashCode());
        result = prime * result + ((subString == null) ? 0 : subString.hashCode());
        result = prime * result + ((tagsSet == null) ? 0 : tagsSet.hashCode());
        result = prime * result + ((topic == null) ? 0 : topic.hashCode());
        result = prime * result + ((expressionType == null) ? 0 : expressionType.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SubscriptionData other = (SubscriptionData) obj;
        if (classFilterMode != other.classFilterMode)
            return false;
        if (codeSet == null) {
            if (other.codeSet != null)
                return false;
        } else if (!codeSet.equals(other.codeSet))
            return false;
        if (subString == null) {
            if (other.subString != null)
                return false;
        } else if (!subString.equals(other.subString))
            return false;
        if (subVersion != other.subVersion)
            return false;
        if (tagsSet == null) {
            if (other.tagsSet != null)
                return false;
        } else if (!tagsSet.equals(other.tagsSet))
            return false;
        if (topic == null) {
            if (other.topic != null)
                return false;
        } else if (!topic.equals(other.topic))
            return false;
        if (expressionType == null) {
            if (other.expressionType != null)
                return false;
        } else if (!expressionType.equals(other.expressionType))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "SubscriptionData [classFilterMode=" + classFilterMode + ", topic=" + topic + ", subString="
            + subString + ", tagsSet=" + tagsSet + ", codeSet=" + codeSet + ", subVersion=" + subVersion
            + ", expressionType=" + expressionType + "]";
    }

    @Override
    public int compareTo(SubscriptionData other) {
        String thisValue = this.topic + "@" + this.subString;
        String otherValue = other.topic + "@" + other.subString;
        return thisValue.compareTo(otherValue);
    }
}
