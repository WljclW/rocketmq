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
package org.apache.rocketmq.remoting.netty;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * Common remoting command processor
 *    处理网络请求 ：接收来自客户端的请求，并根据请求类型执行相应的业务逻辑。这里的客户端包括生产者、消费者，比如：PullMessageProcessor
 * 用于处理消费者拉取消息的请求；SendMessageProcessor处理生产者发送来消息的请求.....
 *    生成响应数据 ：根据请求的处理结果，生成响应数据并返回给客户端。
 *    解耦网络层与业务逻辑 ：通过接口形式将网络通信与具体的业务逻辑分离，便于扩展和维护。
 */
public interface NettyRequestProcessor {
    /**
     * @param ctx 上下文信息包含通道对象
     * @param request 具体的请求，包含请求头 和 请求体
     * @return 处理后的返回信息
     * */
    RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
        throws Exception;

    /**
     * 如果拒绝处理返回true，否则的话返回false
     * */
    boolean rejectRequest();
}
