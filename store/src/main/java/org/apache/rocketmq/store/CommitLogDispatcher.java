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

package org.apache.rocketmq.store;

import org.rocksdb.RocksDBException;

/**
 * Dispatcher of commit log.....
 *      1.即commit log 的分发器，会将CommitLog分发给ConsumeQueue(按照消息主题进行分类)，index(按照消息的
 *          tag进行分类)
 *      2.可以基于这个接口实现我们自定义的类，并在dispatch方法中实现自己的转发逻辑
 *      3.转发时不需要转发消息的具体内容，只需要转发消息的offset等信息。。具体的消息内容只需要在CommitLog中
 *          存储一份即可
 */
public interface CommitLogDispatcher {

    /**
     *  Dispatch messages from store to build consume queues, indexes, and filter data
     * @param request dispatch message request
     * @throws RocksDBException only in rocksdb mode
     */
    void dispatch(final DispatchRequest request) throws RocksDBException;
}
