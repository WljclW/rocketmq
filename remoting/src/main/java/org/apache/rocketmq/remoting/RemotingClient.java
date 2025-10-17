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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.ResponseFuture;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * @description: RocketMQ 客户端通信模块的顶层抽象，定义了与 Broker、NameServer 进行网络通信的所有基本操作，任何组件作为客户端与别的
 *      组件通信时都会用到。
 * @author: Zhou
 * @date: 2025/8/21 22:49
 */
public interface RemotingClient extends RemotingService {
    /**
     * 作用：设置客户端要连接的 NameServer 列表，如 ["192.168.0.1:9876", "192.168.0.2:9876"]
     * 使用场景：
     *      Producer/Consumer 启动时设置 namesrvAddr
     *      动态更新 NameServer 列表（支持高可用）
     * @param addrs
     */
    void updateNameServerAddressList(final List<String> addrs);

    List<String> getNameServerAddressList();

    List<String> getAvailableNameSrvList();

    //向指定地址（如 192.168.0.1:10911）发送一个请求，并同步等待响应
    RemotingCommand invokeSync(final String addr, final RemotingCommand request,
        final long timeoutMillis) throws InterruptedException, RemotingConnectException,
        RemotingSendRequestException, RemotingTimeoutException;

    //作用：发起异步请求，不阻塞线程，通过回调函数接收结果
    void invokeAsync(final String addr, final RemotingCommand request, final long timeoutMillis,
        final InvokeCallback invokeCallback) throws InterruptedException, RemotingConnectException,
        RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

    //作用：发送请求后立即返回，不等待任何响应
    void invokeOneway(final String addr, final RemotingCommand request, final long timeoutMillis)
        throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException,
        RemotingTimeoutException, RemotingSendRequestException;

    /**
     * [对异步发送的封装，避免陷入回调地狱，让异步调用更优雅]
     *      将传统的“回调式异步调用”（Callback-based）封装成“Future式异步调用”（Future-based），返回一
     * 个 CompletableFuture<RemotingCommand>，支持链式调用、超时、异常处理等现代异步编程特性。
     */
    default CompletableFuture<RemotingCommand> invoke(final String addr, final RemotingCommand request,
        final long timeoutMillis) {
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        try {
            invokeAsync(addr, request, timeoutMillis, new InvokeCallback() {

                @Override
                public void operationComplete(ResponseFuture responseFuture) {

                }

                @Override
                public void operationSucceed(RemotingCommand response) {
                    future.complete(response);
                }

                @Override
                public void operationFail(Throwable throwable) {
                    future.completeExceptionally(throwable);
                }
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    /**
     * 作用：为某个请求类型（如 SEND_MESSAGE）注册处理逻辑
     * requestCode 命令编码
     * processor RocketMQ请求业务处理器，例如消息发送的处理器为 SendMessageProcessor，PullMessageProcessor 为消息拉取的业务处理器。
     * executor 线程池，NettyRequestProcessor 具体业务逻辑在该线程池中执行
     */
    void registerProcessor(final int requestCode, final NettyRequestProcessor processor,
        final ExecutorService executor);

    void setCallbackExecutor(final ExecutorService callbackExecutor);

    boolean isChannelWritable(final String addr);

    boolean isAddressReachable(final String addr);

    void closeChannels(final List<String> addrList);
}
