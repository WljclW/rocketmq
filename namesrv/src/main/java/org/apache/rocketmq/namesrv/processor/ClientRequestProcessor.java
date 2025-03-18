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

package org.apache.rocketmq.namesrv.processor;

import com.alibaba.fastjson.serializer.SerializerFeature;
import io.netty.channel.ChannelHandlerContext;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.namesrv.NamesrvUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

public class ClientRequestProcessor implements NettyRequestProcessor {

    private static Logger log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    protected NamesrvController namesrvController;
    private long startupTimeMillis;

    private AtomicBoolean needCheckNamesrvReady = new AtomicBoolean(true);

    public ClientRequestProcessor(final NamesrvController namesrvController) {
        this.namesrvController = namesrvController;
        this.startupTimeMillis = System.currentTimeMillis();
    }

    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx,
        final RemotingCommand request) throws Exception {
        return this.getRouteInfoByTopic(ctx, request);
    }

    /**[]：根据主题（Topic）获取路由信息
     * @param ctx netty的网络通道上下文，用于处理网络请求
     * @param request 请求命令
     * */
    public RemotingCommand getRouteInfoByTopic(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        /*解码请求头，提取出 GetRouteInfoRequestHeader 对象*/
        final GetRouteInfoRequestHeader requestHeader =
            (GetRouteInfoRequestHeader) request.decodeCommandCustomHeader(GetRouteInfoRequestHeader.class);

        boolean namesrvReady = needCheckNamesrvReady.get() && System.currentTimeMillis() - startupTimeMillis >= TimeUnit.SECONDS.toMillis(namesrvController.getNamesrvConfig().getWaitSecondsForService());
        /*如果 NameServer 尚未就绪，记录警告日志，并返回错误响应，提示客户端 NameServer 不可用。*/
        if (namesrvController.getNamesrvConfig().isNeedWaitForService() && !namesrvReady) {
            log.warn("name server not ready. request code {} ", request.getCode());
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("name server not ready");
            return response;
        }
        /*调用 RouteInfoManager.pickupTopicRouteData() 方法，根据主题名称从 NameServer 的路由表中获
        取对应的路由信息（TopicRouteData）*/
        TopicRouteData topicRouteData = this.namesrvController.getRouteInfoManager().pickupTopicRouteData(requestHeader.getTopic());
        /*if块表示找到了对应的路由信息，需要处理一下，然后返回response*/
        if (topicRouteData != null) {
            //topic route info register success ,so disable namesrvReady check
            if (needCheckNamesrvReady.get()) {
                needCheckNamesrvReady.set(false);
            }
            /*如果启用了顺序消息功能（isOrderMessageEnable()），则从 KvConfigManager 中获取该主题的顺序消息
                   配置（orderTopicConf），并将其设置到 TopicRouteData 中*/
            if (this.namesrvController.getNamesrvConfig().isOrderMessageEnable()) {
                String orderTopicConf =
                    this.namesrvController.getKvConfigManager().getKVConfig(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG,
                        requestHeader.getTopic());
                topicRouteData.setOrderTopicConf(orderTopicConf);
            }
            /*根据客户端的版本号或请求头中的 AcceptStandardJsonOnly 字段，决定使用哪种编码方式：
                如果客户端版本较高（>= V4.9.4）或明确要求标准 JSON 格式，则使用更兼容的编码方
                  式（SerializerFeature.BrowserCompatible 等）。
                否则，使用默认的编码方式。将编码后的字节数组存储到 content 中*/
            byte[] content;
            Boolean standardJsonOnly = Optional.ofNullable(requestHeader.getAcceptStandardJsonOnly()).orElse(false);
            if (request.getVersion() >= MQVersion.Version.V4_9_4.ordinal() || standardJsonOnly) {
                content = topicRouteData.encode(SerializerFeature.BrowserCompatible,
                    SerializerFeature.QuoteFieldNames, SerializerFeature.SkipTransientField,
                    SerializerFeature.MapSortField);
            } else {
                content = topicRouteData.encode();
            }

            response.setBody(content);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }
        /*否则表示需要的路由信息没有找到*/
        response.setCode(ResponseCode.TOPIC_NOT_EXIST);
        response.setRemark("No topic route info in name server for the topic: " + requestHeader.getTopic()
            + FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL));
        return response;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
