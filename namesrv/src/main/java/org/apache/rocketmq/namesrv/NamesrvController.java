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
package org.apache.rocketmq.namesrv;

import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.future.FutureTaskExt;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.namesrv.kvconfig.KVConfigManager;
import org.apache.rocketmq.namesrv.processor.ClientRequestProcessor;
import org.apache.rocketmq.namesrv.processor.ClusterTestRequestProcessor;
import org.apache.rocketmq.namesrv.processor.DefaultRequestProcessor;
import org.apache.rocketmq.namesrv.route.ZoneRouteRPCHook;
import org.apache.rocketmq.namesrv.routeinfo.BrokerHousekeepingService;
import org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager;
import org.apache.rocketmq.remoting.Configuration;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.srvutil.FileWatchService;

public class NamesrvController {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);
    private static final Logger WATER_MARK_LOG = LoggerFactory.getLogger(LoggerName.NAMESRV_WATER_MARK_LOGGER_NAME);

    private final NamesrvConfig namesrvConfig;

    private final NettyServerConfig nettyServerConfig;
    private final NettyClientConfig nettyClientConfig;

    private final ScheduledExecutorService scheduledExecutorService = ThreadUtils.newScheduledThreadPool(1,
            new BasicThreadFactory.Builder().namingPattern("NSScheduledThread").daemon(true).build());

    private final ScheduledExecutorService scanExecutorService = ThreadUtils.newScheduledThreadPool(1,
            new BasicThreadFactory.Builder().namingPattern("NSScanScheduledThread").daemon(true).build());

    private final KVConfigManager kvConfigManager;
    private final RouteInfoManager routeInfoManager;

    private RemotingClient remotingClient;
    private RemotingServer remotingServer;

    private final BrokerHousekeepingService brokerHousekeepingService; /*根据通道状态变化，维护路由信息*/

    private ExecutorService defaultExecutor;
    private ExecutorService clientRequestExecutor;

    private BlockingQueue<Runnable> defaultThreadPoolQueue;
    private BlockingQueue<Runnable> clientRequestThreadPoolQueue;

    private final Configuration configuration;
    private FileWatchService fileWatchService;

    public NamesrvController(NamesrvConfig namesrvConfig, NettyServerConfig nettyServerConfig) {
        this(namesrvConfig, nettyServerConfig, new NettyClientConfig());
    }

    /**初始化field，并且用Configuration来记住namesrvConfig、nettyServerConfig的所有配置*/
    public NamesrvController(NamesrvConfig namesrvConfig, NettyServerConfig nettyServerConfig, NettyClientConfig nettyClientConfig) {
        this.namesrvConfig = namesrvConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.nettyClientConfig = nettyClientConfig;
        this.kvConfigManager = new KVConfigManager(this);
        this.brokerHousekeepingService = new BrokerHousekeepingService(this);
        this.routeInfoManager = new RouteInfoManager(namesrvConfig, this);
        this.configuration = new Configuration(LOGGER, this.namesrvConfig, this.nettyServerConfig);
        this.configuration.setStorePathFromConfig(this.namesrvConfig, "configStorePath"); //从namesrvConfig提取”configStorePath“对应的值，并设置到NettyServerConfig中
    }

    public boolean initialize() {   /**做一些初始化操作*/
        loadConfig();   //加载KV配置表...KV配置和其他配置的区别？？
        initiateNetworkComponents();  //创建网络处理组件。包括：remotingClient和remotingServer
        initiateThreadExecutors(); //初始化线程池。一个是clientRequestThreadPoolQueue；另一个是defaultThreadPoolQueue
        registerProcessor();    //注册处理接收到请求的处理器。一个处理请求码为"GET_ROUTE_BY_TOPIC"的请求，一个是defaultProcessor
        startScheduleService(); //开启三个定时任务。一个是定时扫描不活跃broker的路由信息，一个是定时扫描KV配置表，一个是定时打印线程池的水位日志
        initiateSslContext();   //关于ssl通信的文件监听服务创建
        initiateRpcHooks(); //【注意】这种是用户的扩展逻辑，不是jvm钩子函数
        return true;
    }

    private void loadConfig() {
        this.kvConfigManager.load();
    }

    /**
     * 方法启动定时服务，执行以下三个任务：
     *  每隔一段时间扫描不活跃的broker，并清理路由信息
     *  每隔 10 分钟打印所有的KV配置信息
     *  每隔 1 秒打印线程池的水位日志，即客户端请求线程池和默认线程池的队列大小和头部任务的慢时间（从创建到执行的时间）
     * */
    private void startScheduleService() {
        //定期扫描不活跃的broker。默认是每隔5秒
        this.scanExecutorService.scheduleAtFixedRate(NamesrvController.this.routeInfoManager::scanNotActiveBroker,
            5, this.namesrvConfig.getScanNotActiveBrokerInterval(), TimeUnit.MILLISECONDS);
        //定期打印KV配置表。默认是每隔10min打印一次
        this.scheduledExecutorService.scheduleAtFixedRate(NamesrvController.this.kvConfigManager::printAllPeriodically,
            1, 10, TimeUnit.MINUTES);
        //默认每隔一秒打印一次WaterMark(其实就是clientRequestThreadPoolQueue、defaultThreadPoolQueue者两个阻塞队列的状态)
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                NamesrvController.this.printWaterMark();
            } catch (Throwable e) {
                LOGGER.error("printWaterMark error.", e);
            }
        }, 10, 1, TimeUnit.SECONDS);
    }

    /**
     * remotingClient是一个NettyRemotingClient对象，它用于向其他服务发送请求或响应。
     * remotingServer是一个NettyRemotingServer对象，它用于接收和处理来自其他服务的请求或响应。
     * BrokerHousekeepingService对象用于处理broker的连接和断开事件。
     * */
    private void initiateNetworkComponents() {
        /*this.brokerHousekeepingService是一个监听器，用于监听channel状态变化以便在有新连接、出现异常、通道销毁等执行用户自定义逻辑*/
        this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.brokerHousekeepingService);
        this.remotingClient = new NettyRemotingClient(this.nettyClientConfig);
    }

    /**
     * 创建两个阻塞队列，并初始化两个线程池，一个是defaultExecutor，用于处理默认的远程请求；
     *      另一个是clientRequestExecutor，用于处理客户端的路由信息请求。
     * [这两个线程池的区别？]看下面的registerProcessor方法，clientRequestExecutor线程池是特
     *      定的处理器使用的，这种处理器只处理请求码是“GET_ROUTE_BY_TOPIC”的这类请求；另外
     *      一个defaultExecutor线程池是默认处理器处理请求时使用的
     * 这两个线程池都使用了LinkedBlockingQueue作为任务队列；并且继续深入源码的创建过程会发现重写了newTaskFor方法，使
     *      用FutureTaskExt包装了Runnable任务。
     * */
    private void initiateThreadExecutors() {
        this.defaultThreadPoolQueue = new LinkedBlockingQueue<>(this.namesrvConfig.getDefaultThreadPoolQueueCapacity());
        //用于处理通用的远程请求(可以理解为除了“路由信息请求”的其他请求。比如：心跳包，Broker注册)
        this.defaultExecutor = ThreadUtils.newThreadPoolExecutor(this.namesrvConfig.getDefaultThreadPoolNums(), this.namesrvConfig.getDefaultThreadPoolNums(), 1000 * 60, TimeUnit.MILLISECONDS, this.defaultThreadPoolQueue, new ThreadFactoryImpl("RemotingExecutorThread_"));

        this.clientRequestThreadPoolQueue = new LinkedBlockingQueue<>(this.namesrvConfig.getClientRequestThreadPoolQueueCapacity());
        //用于处理客户端的路由信息请求
        this.clientRequestExecutor = ThreadUtils.newThreadPoolExecutor(this.namesrvConfig.getClientRequestThreadPoolNums(), this.namesrvConfig.getClientRequestThreadPoolNums(), 1000 * 60, TimeUnit.MILLISECONDS, this.clientRequestThreadPoolQueue, new ThreadFactoryImpl("ClientRequestExecutorThread_"));
    }

    /*初始化SSL上下文，即配置remotingServer使用TLS协议进行安全通信
    初始化一个文件监听服务，监控 SSL 证书、私钥和信任链文件的变化，并在文件变更时自动重新加载 SslContext，实现证书
            的“热更新”而无需重启 NameServer
     */
    private void initiateSslContext() {
        if (TlsSystemConfig.tlsMode == TlsMode.DISABLED) {
            return;
        }
        //服务器证书、服务器私钥、信任的CA证书
        String[] watchFiles = {TlsSystemConfig.tlsServerCertPath, TlsSystemConfig.tlsServerKeyPath, TlsSystemConfig.tlsServerTrustCertPath};
        //创建监听器。指明文件变化后应该做什么！
        FileWatchService.Listener listener = new FileWatchService.Listener() {
            boolean certChanged, keyChanged = false;

            @Override
            public void onChanged(String path) {
                if (path.equals(TlsSystemConfig.tlsServerTrustCertPath)) {
                    LOGGER.info("The trust certificate changed, reload the ssl context");
                    ((NettyRemotingServer) remotingServer).loadSslContext();
                }
                if (path.equals(TlsSystemConfig.tlsServerCertPath)) {
                    certChanged = true;
                }
                if (path.equals(TlsSystemConfig.tlsServerKeyPath)) {
                    keyChanged = true;
                }
                if (certChanged && keyChanged) { //证书和私钥都变了 → 安全地重新加载（证书文件（.crt）和私钥文件（.key）通常是成对更新的，但是os可能会分两次触发这个事件）
                    LOGGER.info("The certificate and private key changed, reload the ssl context");
                    certChanged = keyChanged = false;
                    ((NettyRemotingServer) remotingServer).loadSslContext();
                }
            }
        };

        try {
            fileWatchService = new FileWatchService(watchFiles, listener);
        } catch (Exception e) {
            LOGGER.warn("FileWatchService created error, can't load the certificate dynamically");
        }
    }

    /**[]：作用是监控 clientRequestThreadPoolQueue 和 defaultThreadPoolQueue 两个线程池队列的当前元素大小 以及 队列中第一个
     *      任务的延迟时间，并记录日志以便分析性能瓶颈或系统负载情况。
     * 打印的参数说明————
     *  this.clientRequestThreadPoolQueue.size()：获取clientRequestThreadPoolQueue线程池队列中当前元素的数量。
     *  this.defaultThreadPoolQueue.size()：获取defaultThreadPoolQueue线程池队列中当前元素的数量。
     *  headSlowTimeMills(this.clientRequestThreadPoolQueue)：获取clientRequestThreadPoolQueue线程池队列中第一个任务的延迟时间，
     *  headSlowTimeMills(this.defaultThreadPoolQueue)：获取defaultThreadPoolQueue线程池队列中第一个任务的延迟时间。
     * */
    private void printWaterMark() {
        WATER_MARK_LOG.info("[WATERMARK] ClientQueueSize:{} ClientQueueSlowTime:{} " + "DefaultQueueSize:{} DefaultQueueSlowTime:{}", this.clientRequestThreadPoolQueue.size(), headSlowTimeMills(this.clientRequestThreadPoolQueue), this.defaultThreadPoolQueue.size(), headSlowTimeMills(this.defaultThreadPoolQueue));
    }

    /**用于计算线程池队列中头部任务（即队列中最先被提交的任务）的等待时间*/
    private long headSlowTimeMills(BlockingQueue<Runnable> q) {
        long slowTimeMills = 0;
        //拿到队列中的第一个任务
        final Runnable firstRunnable = q.peek();

        if (firstRunnable instanceof FutureTaskExt) {
            final Runnable inner = ((FutureTaskExt<?>) firstRunnable).getRunnable();
            if (inner instanceof RequestTask) {
                //计算距离请求创建过了多久
                slowTimeMills = System.currentTimeMillis() - ((RequestTask) inner).getCreateTimestamp();
            }
        }

        if (slowTimeMills < 0) {
            slowTimeMills = 0;
        }

        return slowTimeMills;
    }

    /**
     * []:注册处理器
     * 方法根据 namesrvConfig.isClusterTest() 的值，选择使用ClusterTestRequestProcessor或
     *      者DefaultRequestProcessor作为默认处理器
     * ClusterTestRequestProcessor是一个用于集群测试的处理器，它会在请求前后添加一些环境信息，比如产
     *      品环境名称、请求时间等
     * DefaultRequestProcessor是一个用于正常运行的处理器，它会根据请求的类型，调用不同的方法来处理，比
     *      如注册Broker、获取路由信息、更新配置等。
     * 在 namesrvConfig.isClusterTest() = false 时如果收到请求的 requestCode 等于 RequestCode.GET_ROUTEINFO_BY_TOPIC 则
     *      会使用ClientRequestProcessor来处理；当收到其他请求时，会使用DefaultRequestProcessor来处理。
     * */
    private void registerProcessor() {
        if (namesrvConfig.isClusterTest()) {

            this.remotingServer.registerDefaultProcessor(new ClusterTestRequestProcessor(this, namesrvConfig.getProductEnvName()), this.defaultExecutor);
        } else {
            // Support get route info only temporarily
            ClientRequestProcessor clientRequestProcessor = new ClientRequestProcessor(this);
            this.remotingServer.registerProcessor(RequestCode.GET_ROUTEINFO_BY_TOPIC, clientRequestProcessor, this.clientRequestExecutor);

            this.remotingServer.registerDefaultProcessor(new DefaultRequestProcessor(this), this.defaultExecutor);
        }
    }

    /**方法注册RPC钩子，即在remotingServer处理请求之前或之后执行一些自定义的逻辑*/
    private void initiateRpcHooks() {
        this.remotingServer.registerRPCHook(new ZoneRouteRPCHook());  //ZoneRouteRPCHook目的是实现分区隔离
    }

    /**
     * 【一句话】启动remotingServer、启动remotingClient、启动证书监视服务、启动路由信息管理服务
     * 调用remotingServer对象的start方法，启动一个NettyRemotingServer，用于接收和处理客户端的请求。
     *      如果nettyServerConfig对象的listenPort属性为0，说明是由操作系统自动分配一个可用端口，那么
     *      将remotingServer对象的localListenPort属性赋值给nettyServerConfig对象的listenPort属
     *      性，保持一致。
     * 调用remotingClient对象的updateNameServerAddressList方法，更新本地地址列表，只包含当前机器
     *      的IP地址和端口号。
     * 调用remotingClient对象的start方法，启动一个NettyRemotingClient，用于向其他服务发送请求。
     * 如果fileWatchService对象不为空，调用它的start方法，启动一个文件监视服务，用于动态加载证书文件。
     * 调用routeInfoManager对象的start方法，启动一个路由信息管理器，用于维护Broker和Topic的路由关系。
     * */
    public void start() throws Exception {
        this.remotingServer.start();    //用于接收和处理客户端的请求

        // In test scenarios where it is up to OS to pick up an available port, set the listening port back to config
        // 如果端口号是0（OS随机分配端口），将实际监听的端口写回配置
        if (0 == nettyServerConfig.getListenPort()) {
            nettyServerConfig.setListenPort(this.remotingServer.localListenPort());
        }

        this.remotingClient.updateNameServerAddressList(Collections.singletonList(NetworkUtil.getLocalAddress()
            + ":" + nettyServerConfig.getListenPort()));
        this.remotingClient.start();    //用于向其他服务发送请求

        if (this.fileWatchService != null) {    //实现ssl相关证书、密钥等变化后的动态更新（热加载）
            this.fileWatchService.start();
        }

        this.routeInfoManager.start(); //路由信息管理器启动，目的是维护Broker、Topic和队列的路由关系
    }

    /**
     * 通过这个方法来执行所有的需要关闭的操作，然后将这个方法作为回调添加为jvm钩子，从而实现优雅关闭
     * */
    public void shutdown() {
        this.remotingClient.shutdown();
        this.remotingServer.shutdown();
        this.defaultExecutor.shutdown();
        this.clientRequestExecutor.shutdown();
        this.scheduledExecutorService.shutdown();
        this.scanExecutorService.shutdown();
        this.routeInfoManager.shutdown();

        if (this.fileWatchService != null) {
            this.fileWatchService.shutdown();
        }
    }

    public NamesrvConfig getNamesrvConfig() {
        return namesrvConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    public KVConfigManager getKvConfigManager() {
        return kvConfigManager;
    }

    public RouteInfoManager getRouteInfoManager() {
        return routeInfoManager;
    }

    public RemotingServer getRemotingServer() {
        return remotingServer;
    }

    public RemotingClient getRemotingClient() {
        return remotingClient;
    }

    public void setRemotingServer(RemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }

    public Configuration getConfiguration() {
        return configuration;
    }
}
