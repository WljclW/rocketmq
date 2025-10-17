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
package org.apache.rocketmq.broker;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;

public class BrokerStartup {

    public static Logger log;
    public static final SystemConfigFileHelper CONFIG_FILE_HELPER = new SystemConfigFileHelper();

    public static void main(String[] args) {
        start(createBrokerController(args));
    }

    public static BrokerController start(BrokerController controller) {
        try {
            controller.start();

            String tip = String.format("The broker[%s, %s] boot success. serializeType=%s",
                controller.getBrokerConfig().getBrokerName(), controller.getBrokerAddr(),
                RemotingCommand.getSerializeTypeConfigInThisServer());

            if (null != controller.getBrokerConfig().getNamesrvAddr()) {
                tip += " and name server is " + controller.getBrokerConfig().getNamesrvAddr();
            }

            log.info(tip);
            System.out.printf("%s%n", tip);
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    public static void shutdown(final BrokerController controller) {
        if (null != controller) {
            controller.shutdown();
        }
    }

    /**[]：设置MQ版本号；解析命令行参数 以及 配置文件(会封装到4个对象)，利用封装的4个配置对象创建BrokerController。4个配置对象分
     *      别是brokerConfig、nettyServerConfig、nettyClientConfig、messageStoreConfig
     * */
    public static BrokerController buildBrokerController(String[] args) throws Exception {
        //MQ版本号
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
        /*Broker启动时，需要使用的四个配置类(用于封装用户的配置信息)
             BrokerConfig：Broker的相关配置
             NettyServerConfig：netty服务端的相关配置。封装了作为堆外提供消息读写操作的MQ服务器信息
             NettyClientConfig：作为namesrv客户端的相关配置
             MessageStoreConfig：消息存储相关配置*/
        final BrokerConfig brokerConfig = new BrokerConfig();
        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
        final NettyClientConfig nettyClientConfig = new NettyClientConfig();
        final MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        nettyServerConfig.setListenPort(10911); //设置netty服务端的监听端口
        messageStoreConfig.setHaListenPort(0);

        Options options = ServerUtil.buildCommandlineOptions(new Options()); /*Options有四个主要的集合：longOpts(长表示)、optionGroups、requiresOpts(必须要有的选项)、shortOpts(短表示)*/
        /*解析命令行，完成属性的解析 以及 配置对象属性的初始化*/
        /*1.解析命令行为CommandLine实例*/
        CommandLine commandLine = ServerUtil.parseCmdLine(
            "mqbroker", args, buildCommandlineOptions(options), new DefaultParser());
        if (null == commandLine) {
            System.exit(-1);
        }
        /**从此开始到方法结束前两行，做的事：加载配置文件 以及 读取命令行的解析，将所得配置信息封装到
         * brokerConfig、nettyServerConfig、nettyClientConfig、messageStoreConfig*/
        Properties properties = null;
        /*2.1 处理启动Broker时，命令中的-c参数。拿到conf配置文件，将文件中的配置封装到properties对象中。。如果
        * properties对象不是null，就将这些配置设置到brokerConfig、nettyServerConfig、nettyClientConfig、
        * messageStoreConfig*/
        if (commandLine.hasOption('c')) {
            String file = commandLine.getOptionValue('c');
            if (file != null) {
                CONFIG_FILE_HELPER.setFile(file);
                BrokerPathConfigHelper.setBrokerConfigPath(file); //记录配置路径信息
                properties = CONFIG_FILE_HELPER.loadConfig(); //加载配置信息
            }
        }
        if (properties != null) {
            properties2SystemEnv(properties);
            MixAll.properties2Object(properties, brokerConfig);
            MixAll.properties2Object(properties, nettyServerConfig);
            MixAll.properties2Object(properties, nettyClientConfig);
            MixAll.properties2Object(properties, messageStoreConfig);
        }
        /*2.2 brokerConfig的额外处理。处理其他命令如果brokerConfig需要的话，注入到BrokerConfig中。比如：-n命令；然后
        * 验证一下brokerConfig中所有设置的namesrv地址*/
        MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), brokerConfig);
        if (null == brokerConfig.getRocketmqHome()) {
            System.out.printf("Please set the %s variable in your environment " +
                "to match the location of the RocketMQ installation", MixAll.ROCKETMQ_HOME_ENV);
            System.exit(-2);
        }

        // Validate namesrvAddr
        String namesrvAddr = brokerConfig.getNamesrvAddr();
        if (StringUtils.isNotBlank(namesrvAddr)) {
            try {
                String[] addrArray = namesrvAddr.split(";");
                for (String addr : addrArray) {
                    NetworkUtil.string2SocketAddress(addr);
                }
            } catch (Exception e) {
                System.out.printf("The Name Server Address[%s] illegal, please set it as follows, " +
                        "\"127.0.0.1:9876;192.168.0.1:9876\"%n", namesrvAddr);
                System.exit(-3);
            }
        }
        /*2.3 messageStoreConfig的相关额外处理。
                ①如果Broker的角色是从，设置“消息占用内存的最大比率”比默认值再小10%；
                ②brokerConfig允许角色切换，则设置 以及 验证brokerId是不是合规
                ③如果是集群自主决策，则brokerId是-1*/
        if (BrokerRole.SLAVE == messageStoreConfig.getBrokerRole()) {
            int ratio = messageStoreConfig.getAccessMessageInMemoryMaxRatio() - 10;
            messageStoreConfig.setAccessMessageInMemoryMaxRatio(ratio);
        }

        // Set broker role according to ha config
        if (!brokerConfig.isEnableControllerMode()) {
            switch (messageStoreConfig.getBrokerRole()) {
                case ASYNC_MASTER:
                case SYNC_MASTER:
                    brokerConfig.setBrokerId(MixAll.MASTER_ID);
                    break;
                case SLAVE:
                    if (brokerConfig.getBrokerId() <= MixAll.MASTER_ID) {
                        System.out.printf("Slave's brokerId must be > 0%n");
                        System.exit(-3);
                    }
                    break;
                default:
                    break;
            }
        }

        if (messageStoreConfig.isEnableDLegerCommitLog()) {
            brokerConfig.setBrokerId(-1);
        }
        /*为什么下面的是互斥逻辑————
        brokerConfig.isEnableControllerMode()
            含义：是否启用 Controller 模式（RocketMQ 5.0+ 引入的新架构）
            作用：
                Broker 不再自己管理主从角色（Master/Slave），角色由外部的 Controller 集群统一管理
                实现 存算分离：计算层（Broker）与控制层（Controller）分离
        messageStoreConfig.isEnableDLegerCommitLog()
            含义：是否启用 DLedger 模式（RocketMQ 4.5+ 引入）
            作用：
                   使用 DLedger（基于 Raft 协议） 实现主从自动切换。多个 Broker 组成一个组，通过投票选
                出主节点，消息通过 DLedger 复制日志保证一致性
         “要么用 DLedger 自己选主，要么让 Controller 来管你，不能两头都听！” ，如果都配置为true，但是两者返回的结果不一样，这
         种情况下怎么处理？应该听谁的？因此rocketmq直接拦截这种错误配置
         */
        if (brokerConfig.isEnableControllerMode() && messageStoreConfig.isEnableDLegerCommitLog()) {
            System.out.printf("The config enableControllerMode and enableDLegerCommitLog cannot both be true.%n");
            System.exit(-4);
        }
        //高可用端口：要么使用默认值10912；要麽用户自定义但要求大于0
        if (messageStoreConfig.getHaListenPort() <= 0) {
            messageStoreConfig.setHaListenPort(nettyServerConfig.getListenPort() + 1);
        }

        brokerConfig.setInBrokerContainer(false);

        System.setProperty("brokerLogDir", "");
        if (brokerConfig.isIsolateLogEnable()) {
            System.setProperty("brokerLogDir", brokerConfig.getBrokerName() + "_" + brokerConfig.getBrokerId());
        }
        if (brokerConfig.isIsolateLogEnable() && messageStoreConfig.isEnableDLegerCommitLog()) {
            System.setProperty("brokerLogDir", brokerConfig.getBrokerName() + "_" + messageStoreConfig.getdLegerSelfId());
        }

        if (commandLine.hasOption('p')) {
            Logger console = LoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);
            MixAll.printObjectProperties(console, brokerConfig);
            MixAll.printObjectProperties(console, nettyServerConfig);
            MixAll.printObjectProperties(console, nettyClientConfig);
            MixAll.printObjectProperties(console, messageStoreConfig);
            System.exit(0);
        } else if (commandLine.hasOption('m')) {
            Logger console = LoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);
            MixAll.printObjectProperties(console, brokerConfig, true);
            MixAll.printObjectProperties(console, nettyServerConfig, true);
            MixAll.printObjectProperties(console, nettyClientConfig, true);
            MixAll.printObjectProperties(console, messageStoreConfig, true);
            System.exit(0);
        }
        /*设置配置类的日志*/
        log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
        MixAll.printObjectProperties(log, brokerConfig);
        MixAll.printObjectProperties(log, nettyServerConfig);
        MixAll.printObjectProperties(log, nettyClientConfig);
        MixAll.printObjectProperties(log, messageStoreConfig);
        /**根据四个配置类，创建BrokerController*/
        final BrokerController controller = new BrokerController(
            brokerConfig, nettyServerConfig, nettyClientConfig, messageStoreConfig);

        // Remember all configs to prevent discard。。配置信息 缓存到Broker本地内存
        controller.getConfiguration().registerConfig(properties);

        return controller;
    }

    public static Runnable buildShutdownHook(BrokerController brokerController) {
        return new Runnable() {
            private volatile boolean hasShutdown = false;
            private final AtomicInteger shutdownTimes = new AtomicInteger(0);

            @Override
            public void run() {
                synchronized (this) {
                    log.info("Shutdown hook was invoked, {}", this.shutdownTimes.incrementAndGet());
                    if (!this.hasShutdown) {
                        this.hasShutdown = true;
                        long beginTime = System.currentTimeMillis();
                        brokerController.shutdown();
                        long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                        log.info("Shutdown hook over, consuming total time(ms): {}", consumingTimeTotal);
                    }
                }
            }
        };
    }

    public static BrokerController createBrokerController(String[] args) {
        try {
            /*buildBrokerController方法完成命令行、配置文件的解析；利用解析的结果new一个BrokerController对象*/
            BrokerController controller = buildBrokerController(args);
            /*BrokerController的初始化*/
            boolean initResult = controller.initialize();
            if (!initResult) {
                controller.shutdown(); //如果初始化失败就停止后续步骤
                System.exit(-3);
            }
            Runtime.getRuntime().addShutdownHook(new Thread(buildShutdownHook(controller))); /*注册jvm虚拟机关闭时执行的钩子函数*/
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return null;
    }

    /**
     * 支持一种 基于域名的 NameServer 地址自动发现机制
     * @param properties 读取到的配置文件信息
     */
    private static void properties2SystemEnv(Properties properties) {
        if (properties == null) {
            return;
        }
        String rmqAddressServerDomain = properties.getProperty("rmqAddressServerDomain", MixAll.WS_DOMAIN_NAME);
        String rmqAddressServerSubGroup = properties.getProperty("rmqAddressServerSubGroup", MixAll.WS_DOMAIN_SUBGROUP);
        System.setProperty("rocketmq.namesrv.domain", rmqAddressServerDomain);
        System.setProperty("rocketmq.namesrv.domain.subgroup", rmqAddressServerSubGroup);
    }

    private static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Broker config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("m", "printImportantConfig", false, "Print important config item");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    public static class SystemConfigFileHelper {
        private static final Logger LOGGER = LoggerFactory.getLogger(SystemConfigFileHelper.class);

        private String file;

        public SystemConfigFileHelper() {
        }

        public Properties loadConfig() throws Exception {
            InputStream in = new BufferedInputStream(Files.newInputStream(Paths.get(file)));
            Properties properties = new Properties();
            properties.load(in);
            in.close();
            return properties;
        }

        public void update(Properties properties) throws Exception {
            LOGGER.error("[SystemConfigFileHelper] update no thing.");
        }

        public void setFile(String file) {
            this.file = file;
        }

        public String getFile() {
            return file;
        }
    }
}
