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
package org.apache.rocketmq.remoting;

import io.netty.channel.Channel;
import java.util.concurrent.ExecutorService;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * 关于RemotingServer和RemotingClient的说明：
 *      RomotingClient主要是Producer发送消息与Consumer拉取消息时用到；RomotingServer主要是Broker进行回
 *      调，获取Consumer状态等的时候用到。
 *      这里重点需要关注下registerProcessor注册命令处理器这个方法。RocketMQ 会按照业务逻辑进行拆分，例如
 *      消息发送、消息拉取等每一个网络操作会定义一个请求编码（requestCode），然后每一个类型对应一个业务处
 *      理器 NettyRequestProcessor，并可以按照不同的 requestCode 定义不同的线程池，实现不同请求的线程池
 *      隔离。
 * */

public interface RemotingServer extends RemotingService {

    void registerProcessor(final int requestCode, final NettyRequestProcessor processor,
        final ExecutorService executor);

    void registerDefaultProcessor(final NettyRequestProcessor processor, final ExecutorService executor);

    int localListenPort();

    //根据请求编码获取对应的请求业务处理器与线程池
    Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(final int requestCode);

    Pair<NettyRequestProcessor, ExecutorService> getDefaultProcessorPair();

    RemotingServer newRemotingServer(int port);

    void removeRemotingServer(int port);

    RemotingCommand invokeSync(final Channel channel, final RemotingCommand request,
        final long timeoutMillis) throws InterruptedException, RemotingSendRequestException,
        RemotingTimeoutException;

    void invokeAsync(final Channel channel, final RemotingCommand request, final long timeoutMillis,
        final InvokeCallback invokeCallback) throws InterruptedException,
        RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

    void invokeOneway(final Channel channel, final RemotingCommand request, final long timeoutMillis)
        throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException,
        RemotingSendRequestException;

}
