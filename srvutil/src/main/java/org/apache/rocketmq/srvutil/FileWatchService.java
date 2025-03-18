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

package org.apache.rocketmq.srvutil;

import com.google.common.base.Strings;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.common.LifecycleAwareServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * FileWatchService文件变更监听服务实现逻辑，内部维护了需要监听的文件列表、监听文件列表的hash消息摘要、监听器。当调用线程的
 *      start()函数后，就会执行当前类的run(函数)，只要系统没有停止，就会无限循环切间隔形式.扫描文件又没有变更。如果有变更，
 *      则将它维护到内存列表，并且调用消息监听器changed()回调函数。
*/
public class FileWatchService extends LifecycleAwareServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);
    /*需要监听的文件——>该文件对应的哈希值*/
    private final Map<String, String> currentHash = new HashMap<>();
    private final Listener listener;
    private static final int WATCH_INTERVAL = 500;
    private final MessageDigest md = MessageDigest.getInstance("MD5");

    /**[]:1.初始化listener
     *  2.遍历形参的文件列表；计算每一个文件的摘要；并将 文件名——>摘要 添加到currentHash这个map中*/
    public FileWatchService(final String[] watchFiles,
        final Listener listener) throws Exception {
        this.listener = listener;
        for (String file : watchFiles) {
            if (!Strings.isNullOrEmpty(file) && new File(file).exists()) {
                currentHash.put(file, md5Digest(file));
            }
        }
    }

    @Override
    public String getServiceName() {
        return "FileWatchService";
    }

    /**[]:该服务启动后就是不断的执行这个runo()。最根处的地方其实是从父类LifecycleAwareServiceThread的run()方法执
     *      行的，然后父类的方法调用到了子类具体实现的run0().——有点类似于模板方法。
     * */
    @Override
    public void run0() {
        log.info(this.getServiceName() + " service started");
        /*如果服务没有终止，就不出while。*/
        while (!this.isStopped()) {
            try {
                /*首先，默认间隔500ms执行一次*/
                this.waitForRunning(WATCH_INTERVAL);
                /*遍历currentHash这个map，如果发现某个文件的摘要值和之前计算的不一致，则调用listener的onChanged()方法；
                 * 并且更新currentHash这个map。*/
                for (Map.Entry<String, String> entry : currentHash.entrySet()) {
                    String newHash = md5Digest(entry.getKey());
                    if (!newHash.equals(currentHash.get(entry.getKey()))) {
                        entry.setValue(newHash);
                        listener.onChanged(entry.getKey());
                    }
                }
            } catch (Exception e) {
                log.warn(this.getServiceName() + " service raised an unexpected exception.", e);
            }
        }
        log.info(this.getServiceName() + " service end");
    }

    /**
     * Note: we ignore DELETE event on purpose. This is useful when application renew CA file.
     * When the operator delete/rename the old CA file and copy a new one, this ensures the old CA file is used during
     * the operation.
     * <p>
     * As we know exactly what to do when file does not exist or when IO exception is raised, there is no need to
     * propagate the exception up.
     *
     * @param filePath Absolute path of the file to calculate its MD5 digest.
     * @return Hash of the file content if exists; empty string otherwise.
     */
    private String md5Digest(String filePath) {
        Path path = Paths.get(filePath);
        if (!path.toFile().exists()) {
            // Reuse previous hash result
            return currentHash.getOrDefault(filePath, "");
        }
        byte[] raw;
        try {
            raw = Files.readAllBytes(path);
        } catch (IOException e) {
            log.info("Failed to read content of {}", filePath);
            // Reuse previous hash result
            return currentHash.getOrDefault(filePath, "");
        }
        md.update(raw);
        byte[] hash = md.digest();
        return UtilAll.bytes2string(hash);
    }

    public interface Listener {
        /**
         * Will be called when the target files are changed
         *
         * @param path the changed file path
         */
        void onChanged(String path);
    }
}
