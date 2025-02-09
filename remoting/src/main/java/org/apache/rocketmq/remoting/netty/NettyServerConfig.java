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

public class NettyServerConfig implements Cloneable {

    /**
     * Bind address may be hostname, IPv4 or IPv6.
     * By default, it's wildcard address, listening all network interfaces.
     */
    private String bindAddress = "0.0.0.0";     // NameServer 默认绑定地址
    private int listenPort = 0; //NameServer监听端口，默认初始化为9876
    private int serverWorkerThreads = 8;    // Netty 业务线程池线程个数
    //业务线程池
    private int serverCallbackExecutorThreads = 0;  // Netty public 任务线程池线程个数， Netty 网络根据业务类型会创建不同的线程池，比如处理消息发送、消息消费、心跳检测等。如果该业务类型（RequestCode）未注册线程池， 则由 public线程池执行
    //解析请求并转发给特定的业务线程池
    private int serverSelectorThreads = 3;  //  IO 线程池线程个数，主要是 NameServer、Broker 端解析请求、返回相应的线程个数。。这类线程主要是处理网络请求的，解析请求包， 然后转发到各个业务线程池完成具体的业务操作，然后将结果再返回调用方
    private int serverOnewaySemaphoreValue = 256;   // send oneway 消息请求的并发度（Broker 端参数）
    private int serverAsyncSemaphoreValue = 64;     // 异步消息发送最大并发度（Broker端参数）
    private int serverChannelMaxIdleTimeSeconds = 120;  // 网络连接所允许的最大空闲时间，默认120s。如果连接空闲时间超过该参数设置的值，连接将被关闭

    private int serverSocketSndBufSize = NettySystemConfig.socketSndbufSize;    // 网络 socket 发送缓存区大小，默认 64k
    private int serverSocketRcvBufSize = NettySystemConfig.socketRcvbufSize;    // 网络 socket 接受缓存区大小，默认 64k
    /**
     * 高水位标记，当Channel的待写入数据量达到此值时，Netty会自动关闭Channel的写操作，
     * 需要用户手动调用Channel的flush方法来刷新缓冲区以继续写入数据。
     * 这有助于防止应用程序过度缓冲数据，导致内存使用过多。
     */
    private int writeBufferHighWaterMark = NettySystemConfig.writeBufferHighWaterMark;
    /**
     * 低水位标记，当Channel的待写入数据量减少到此值时，Netty会自动重新打开Channel的写操作，
     * 允许数据再次被写入缓冲区。
     * 这有助于在数据量减少到一个合理水平时恢复写操作，保证数据传输的流畅。
     */
    private int writeBufferLowWaterMark = NettySystemConfig.writeBufferLowWaterMark;
    private int serverSocketBacklog = NettySystemConfig.socketBacklog;  // 同时处理的连接请求的最大数量默认1024
//    private boolean serverNettyWorkerGroupEnable = true;    // ByteBuffer是否开启缓存，建议开启
    private boolean serverPooledByteBufAllocatorEnable = true;  // 是否启用Epoll IO模型，Linux环境建议开启

    private boolean enableShutdownGracefully = false;
    private int shutdownWaitTimeSeconds = 30;

    /**
     * make install
     *
     *
     * ../glibc-2.10.1/configure \ --prefix=/usr \ --with-headers=/usr/include \
     * --host=x86_64-linux-gnu \ --build=x86_64-pc-linux-gnu \ --without-gd
     */
    private boolean useEpollNativeSelector = false;

    public String getBindAddress() {
        return bindAddress;
    }

    public void setBindAddress(String bindAddress) {
        this.bindAddress = bindAddress;
    }

    public int getListenPort() {
        return listenPort;
    }

    public void setListenPort(int listenPort) {
        this.listenPort = listenPort;
    }

    public int getServerWorkerThreads() {
        return serverWorkerThreads;
    }

    public void setServerWorkerThreads(int serverWorkerThreads) {
        this.serverWorkerThreads = serverWorkerThreads;
    }

    public int getServerSelectorThreads() {
        return serverSelectorThreads;
    }

    public void setServerSelectorThreads(int serverSelectorThreads) {
        this.serverSelectorThreads = serverSelectorThreads;
    }

    public int getServerOnewaySemaphoreValue() {
        return serverOnewaySemaphoreValue;
    }

    public void setServerOnewaySemaphoreValue(int serverOnewaySemaphoreValue) {
        this.serverOnewaySemaphoreValue = serverOnewaySemaphoreValue;
    }

    public int getServerCallbackExecutorThreads() {
        return serverCallbackExecutorThreads;
    }

    public void setServerCallbackExecutorThreads(int serverCallbackExecutorThreads) {
        this.serverCallbackExecutorThreads = serverCallbackExecutorThreads;
    }

    public int getServerAsyncSemaphoreValue() {
        return serverAsyncSemaphoreValue;
    }

    public void setServerAsyncSemaphoreValue(int serverAsyncSemaphoreValue) {
        this.serverAsyncSemaphoreValue = serverAsyncSemaphoreValue;
    }

    public int getServerChannelMaxIdleTimeSeconds() {
        return serverChannelMaxIdleTimeSeconds;
    }

    public void setServerChannelMaxIdleTimeSeconds(int serverChannelMaxIdleTimeSeconds) {
        this.serverChannelMaxIdleTimeSeconds = serverChannelMaxIdleTimeSeconds;
    }

    public int getServerSocketSndBufSize() {
        return serverSocketSndBufSize;
    }

    public void setServerSocketSndBufSize(int serverSocketSndBufSize) {
        this.serverSocketSndBufSize = serverSocketSndBufSize;
    }

    public int getServerSocketRcvBufSize() {
        return serverSocketRcvBufSize;
    }

    public void setServerSocketRcvBufSize(int serverSocketRcvBufSize) {
        this.serverSocketRcvBufSize = serverSocketRcvBufSize;
    }

    public int getServerSocketBacklog() {
        return serverSocketBacklog;
    }

    public void setServerSocketBacklog(int serverSocketBacklog) {
        this.serverSocketBacklog = serverSocketBacklog;
    }

    public boolean isServerPooledByteBufAllocatorEnable() {
        return serverPooledByteBufAllocatorEnable;
    }

    public void setServerPooledByteBufAllocatorEnable(boolean serverPooledByteBufAllocatorEnable) {
        this.serverPooledByteBufAllocatorEnable = serverPooledByteBufAllocatorEnable;
    }

    public boolean isUseEpollNativeSelector() {
        return useEpollNativeSelector;
    }

    public void setUseEpollNativeSelector(boolean useEpollNativeSelector) {
        this.useEpollNativeSelector = useEpollNativeSelector;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return (NettyServerConfig) super.clone();
    }

    public int getWriteBufferLowWaterMark() {
        return writeBufferLowWaterMark;
    }

    public void setWriteBufferLowWaterMark(int writeBufferLowWaterMark) {
        this.writeBufferLowWaterMark = writeBufferLowWaterMark;
    }

    public int getWriteBufferHighWaterMark() {
        return writeBufferHighWaterMark;
    }

    public void setWriteBufferHighWaterMark(int writeBufferHighWaterMark) {
        this.writeBufferHighWaterMark = writeBufferHighWaterMark;
    }

    public boolean isEnableShutdownGracefully() {
        return enableShutdownGracefully;
    }

    public void setEnableShutdownGracefully(boolean enableShutdownGracefully) {
        this.enableShutdownGracefully = enableShutdownGracefully;
    }

    public int getShutdownWaitTimeSeconds() {
        return shutdownWaitTimeSeconds;
    }

    public void setShutdownWaitTimeSeconds(int shutdownWaitTimeSeconds) {
        this.shutdownWaitTimeSeconds = shutdownWaitTimeSeconds;
    }
}
