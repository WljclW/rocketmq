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
package org.apache.rocketmq.common;

import org.apache.rocketmq.common.config.RocksDBConfigManager;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.rocksdb.Statistics;

import java.io.IOException;
import java.util.Map;

/**用于管理和维护配置信息。它提供了一种通用的方式来加载、保存和更新
 * 配置，并支持将配置持久化到文件中或从文件中加载。*/
public abstract class ConfigManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    protected RocksDBConfigManager rocksDBConfigManager;

    /**【作用】加载子类方法configFilePath()指定的文件，并调用decode方法解析(decode方法由子类重写)。因此
     *      这个方法在父类就相当于流程规定好了，但是具体的操作 或者 数据由子类提供————模板方法
     * 【总结】类似于模板方法，父类这里制定了加载的流程，但是具体的文件名等信息是子类通过重写父类的
     *      某些方法提供。。从子类中拿到文件名(如果有非空内容)，则尝试加载...下然后会调用decode方
     *      法（这是一个抽象方法，要子类重写），这个方法的逻辑就是根据得到的jsonString初始化自己的字段，
     *      由于子类需要重写，因此往往这种初始化的字段就是子类需要的某些属性。。
     *          以“ConsumerOffsetManager#decode”为例做了注释，其他的持久化文件的加载也是类似的道理.
     * 【流程】1. 调用子类重写的configFilePath()拿到文件路径 并且 读取文件内容为字符串
     *       2. 如果解析到的字符串不是null则解析 JSON 字符串到当前对象（如果为空则使用备份文件this.loadBak()）*/
    public boolean load() {
        String fileName = null;
        try {
            //执行子类重写的configFilePath()，拿到文件名
            fileName = this.configFilePath();
            String jsonString = MixAll.file2String(fileName);

            if (null == jsonString || jsonString.length() == 0) {
                return this.loadBak();
            } else {
                this.decode(jsonString);
                log.info("load " + fileName + " OK");
                return true;
            }
        } catch (Exception e) {
            log.error("load " + fileName + " failed, and try to load backup file", e);
            return this.loadBak();
        }
    }

    private boolean loadBak() {
        String fileName = null;
        try {
            fileName = this.configFilePath();
            String jsonString = MixAll.file2String(fileName + ".bak");
            if (jsonString != null && jsonString.length() > 0) {
                this.decode(jsonString);
                log.info("load " + fileName + " OK");
                return true;
            }
        } catch (Exception e) {
            log.error("load " + fileName + " Failed", e);
            return false;
        }

        return true;
    }

    public synchronized <T> void persist(String topicName, T t) {
        // stub for future
        this.persist();
    }

    public synchronized <T> void persist(Map<String, T> m) {
        // stub for future
        this.persist();
    }

    /**【功能】将当前的配置对象持久化到文件中。。。。
     * 【思考】
     *      1.这个方法提供了一种持久化的方式，以后用到持久化时可以参考从这个方法开始的逻辑*/
    public synchronized void persist() {
        String jsonString = this.encode(true);
        if (jsonString != null) {
            String fileName = this.configFilePath();
            try {
                MixAll.string2File(jsonString, fileName);
            } catch (IOException e) {
                log.error("persist file " + fileName + " exception", e);
            }
        }
    }

    protected void decode0(final byte[] key, final byte[] body) {

    }

    public boolean stop() {
        return true;
    }

    public abstract String configFilePath();

    public abstract String encode();

    public abstract String encode(final boolean prettyFormat);

    public abstract void decode(final String jsonString);

    public Statistics getStatistics() {
        return rocksDBConfigManager == null ? null : rocksDBConfigManager.getStatistics();
    }
}
