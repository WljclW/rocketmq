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
package org.apache.rocketmq.namesrv.routeinfo;

import io.netty.channel.Channel;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.ChannelEventListener;

/**[]:关键在于实现了ChannelEventListener接口，因此namesrv与Broker的通道在变化的时候，能够在监听的回调中更新路由信息
 *    1.用于管理 Broker 的生命周期事件（如连接建立、断开等）。它是 ChannelEventListener 的一个实现类——专门用于监听和处
 *      理与 Broker 通道相关的事件！！！！通过 BrokerHousekeepingService，NameServer能够动态维护集群的路由信息，并在
 *      Broker离线或异常时及时更新路由表(Broker的离线服务即BatchUnregistrationService)。
 *    2.方法的主要逻辑：在通道的状态出现异常、关闭、空闲时，执行相关的回调。之所以能维护路由信息，是因为这些回调的方法中实质
 *      上是调用了RouteInfoManager的相关方法*/
public class BrokerHousekeepingService implements ChannelEventListener {

    private final NamesrvController namesrvController;

    public BrokerHousekeepingService(NamesrvController namesrvController) {
        this.namesrvController = namesrvController;
    }

    @Override
    public void onChannelConnect(String remoteAddr, Channel channel) {
    }

    @Override
    public void onChannelClose(String remoteAddr, Channel channel) {
        this.namesrvController.getRouteInfoManager().onChannelDestroy(channel);
    }

    @Override
    public void onChannelException(String remoteAddr, Channel channel) {
        this.namesrvController.getRouteInfoManager().onChannelDestroy(channel); //通道抛异常时将通道关闭
    }

    @Override
    public void onChannelIdle(String remoteAddr, Channel channel) {
        this.namesrvController.getRouteInfoManager().onChannelDestroy(channel);
    }

    @Override
    public void onChannelActive(String remoteAddr, Channel channel) {

    }
}
