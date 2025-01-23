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

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.JraftConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.controller.ControllerManager;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.srvutil.ShutdownHookThread;

public class NamesrvStartup {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);
    private static final Logger logConsole = LoggerFactory.getLogger(LoggerName.NAMESRV_CONSOLE_LOGGER_NAME);
    private static Properties properties = null;
    private static NamesrvConfig namesrvConfig = null;
    private static NettyServerConfig nettyServerConfig = null;
    private static NettyClientConfig nettyClientConfig = null;
    private static ControllerConfig controllerConfig = null;

    public static void main(String[] args) {
        main0(args);
        controllerManagerMain();
    }

    public static NamesrvController main0(String[] args) {
        try {
            parseCommandlineAndConfigFile(args);    //解吸命令行参数和配置文件
            NamesrvController controller = createAndStartNamesrvController();   //创建并启动namesrv控制器(NameServerController 实例为NameSerer 核心控制器)
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    public static ControllerManager controllerManagerMain() {
        try {
            if (namesrvConfig.isEnableControllerInNamesrv()) {
                return createAndStartControllerManager();
            }
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return null;
    }


    /**
     * 1.解析命令行参数和配置文件
     * 2.该方法首先设置Remoting框架的版本属性，然后解析命令行参数，接着加载配置文件（如果有提供）
     * 3.最后，根据命令行参数和配置文件初始化相关的配置对象
     * 4.带有-c参数即指定了配置文件，此时会拿出配置文件的东西赋值给相应的xxxxConfig对象，这些对象主要包括：namesrvConfig、
     *      nettyServerConfig、nettyClientConfig、controllerConfig、JraftConfig。
     * 5.如何根据 配置文件 的东西赋值给对象？通过反射拿到对象的setAbcd方法，判断abcd这个属性是不是在配置文件中，如果在的话
     *      就将对应的值拿出来，通过反射执行xxxConfig的setAbcd方法完成赋值.
     * 6.-p参数是打印配置信息,就是将上面XxxxConfig对应的状态信息(即对象的字段值)打印出来.
     *
     * @param args 命令行参数数组
     * @throws Exception 如果解析命令行参数或加载配置文件时发生错误，则抛出异常
     */
    public static void parseCommandlineAndConfigFile(String[] args) throws Exception {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
        //构建命令行选项，添加 -h -n选项
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        //添加-c -p解析命令行参数
        CommandLine commandLine = ServerUtil.parseCmdLine("mqnamesrv", args, buildCommandlineOptions(options), new DefaultParser());
        if (null == commandLine) {
            System.exit(-1);
            return;
        }

        //下面三行初始化配置对象
        namesrvConfig = new NamesrvConfig();
        nettyServerConfig = new NettyServerConfig();
        nettyClientConfig = new NettyClientConfig();
        nettyServerConfig.setListenPort(9876);  //// 设置Netty服务器的监听端口  也就是默认的 NameServer 端口是 9876
        //-c选项可以指定namesrv配置文件的地址。根据指定的文件拿出属性赋给xxxxxConfig对象的对应字段
        if (commandLine.hasOption('c')) {
            String file = commandLine.getOptionValue('c');
            if (file != null) {
                InputStream in = new BufferedInputStream(Files.newInputStream(Paths.get(file)));
                properties = new Properties();
                properties.load(in);
                /**
                 * 将配置文件中的属性通过反射setter方法设置到namesrvConfig、nettyServerConfig、nettyClientConfig
                 * 中
                 * */
                MixAll.properties2Object(properties, namesrvConfig);
                MixAll.properties2Object(properties, nettyServerConfig);
                MixAll.properties2Object(properties, nettyClientConfig);
                if (namesrvConfig.isEnableControllerInNamesrv()) {  // 如果配置中启用了控制器功能，初始化并配置控制器
                    controllerConfig = new ControllerConfig();
                    JraftConfig jraftConfig = new JraftConfig();
                    controllerConfig.setJraftConfig(jraftConfig);
                    MixAll.properties2Object(properties, controllerConfig);
                    MixAll.properties2Object(properties, jraftConfig);
                }
                namesrvConfig.setConfigStorePath(file);     // 设置配置文件路径

                System.out.printf("load config properties file OK, %s%n", file);
                in.close();
            }
        }
        //将命令行选项的完整名称（eg：-n-->namesrvAddr）对应的选项值设置到NameSrvConfig的对应属性上
        MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), namesrvConfig);
        //-p选项可以将所有的属性条目打印在控制台
        if (commandLine.hasOption('p')) {
            MixAll.printObjectProperties(logConsole, namesrvConfig);
            MixAll.printObjectProperties(logConsole, nettyServerConfig);
            MixAll.printObjectProperties(logConsole, nettyClientConfig);
            if (namesrvConfig.isEnableControllerInNamesrv()) {  //同样如果配置文件设置了raft配置，也需要打印出关于它的配置
                MixAll.printObjectProperties(logConsole, controllerConfig);
            }
            System.exit(0);
        }

        if (null == namesrvConfig.getRocketmqHome()) {
            System.out.printf("Please set the %s variable in your environment to match the location of the RocketMQ installation%n", MixAll.ROCKETMQ_HOME_ENV);
            System.exit(-2);
        }
        //日志打印NamesrvConfig和NettyServerConfig和配置
        MixAll.printObjectProperties(log, namesrvConfig);
        MixAll.printObjectProperties(log, nettyServerConfig);

    }

    public static NamesrvController createAndStartNamesrvController() throws Exception {

        NamesrvController controller = createNamesrvController();   // 创建 NameServer 控制器实例
        start(controller);
        NettyServerConfig serverConfig = controller.getNettyServerConfig();     //获取配置文件用于打印成功信息
        String tip = String.format("The Name Server boot success. serializeType=%s, address %s:%d", RemotingCommand.getSerializeTypeConfigInThisServer(), serverConfig.getBindAddress(), serverConfig.getListenPort());
        log.info(tip);      // 记录启动日志
        System.out.printf("%s%n", tip);     // 控制台输出启动信息
        return controller;
    }

    /**
     * 这个方法的作用就是创建controller,在构造方法中会初始化自己的Configuration字段,但是在Configuration对象创建时的构造方法中会
     *      完成registerConfig方法,会将namesrvConfig,nettyServerConfig的指定条件的字段值记录在Configuration.allConfigs中.
     * */
    public static NamesrvController createNamesrvController() {

        final NamesrvController controller = new NamesrvController(namesrvConfig, nettyServerConfig, nettyClientConfig);
        // remember all configs to prevent discard
        controller.getConfiguration().registerConfig(properties);
        return controller;
    }

    /**
     * 主要做三件事：
     *  ①初始化NamesrvController
     *  ②添加钩子函数
     *  ③start启动NamesrvController(包括RemotingServer、RemotingClient、RouteInfoManager的启动)
     * */
    public static NamesrvController start(final NamesrvController controller) throws Exception {
        // 检查传入的 NamesrvController 实例是否为null，如果是，则抛出异常。。。换句话说，到这里要确保NamesrvController已经初始化完成
        if (null == controller) {
            throw new IllegalArgumentException("NamesrvController is null");
        }
        //初始化控制器。初始化 NamesrvController，如果初始化失败，则关闭控制器并退出程序
        boolean initResult = controller.initialize();
        if (!initResult) {  //初始化失败并关闭控制器
            controller.shutdown();
            System.exit(-3);
        }
        //添加关闭钩子，用于实现优雅关闭。。方法中调用各个零件的shutdown方法，在各个组件的方法中实现优雅关闭的逻辑
        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(log, (Callable<Void>) () -> {
            controller.shutdown();
            return null;
        }));

        controller.start();     //启动控制器

        return controller;      // 返回启动后的 NamesrvController 实例
    }

    public static ControllerManager createAndStartControllerManager() throws Exception {
        ControllerManager controllerManager = createControllerManager();
        start(controllerManager);
        String tip = "The ControllerManager boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();
        log.info(tip);
        System.out.printf("%s%n", tip);
        return controllerManager;
    }

    public static ControllerManager createControllerManager() throws Exception {
        NettyServerConfig controllerNettyServerConfig = (NettyServerConfig) nettyServerConfig.clone();
        ControllerManager controllerManager = new ControllerManager(controllerConfig, controllerNettyServerConfig, nettyClientConfig);
        // remember all configs to prevent discard
        controllerManager.getConfiguration().registerConfig(properties);
        return controllerManager;
    }

    public static ControllerManager start(final ControllerManager controllerManager) throws Exception {

        if (null == controllerManager) {
            throw new IllegalArgumentException("ControllerManager is null");
        }

        boolean initResult = controllerManager.initialize();
        if (!initResult) {
            controllerManager.shutdown();
            System.exit(-3);
        }

        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(log, (Callable<Void>) () -> {
            controllerManager.shutdown();
            return null;
        }));

        controllerManager.start();

        return controllerManager;
    }

    public static void shutdown(final NamesrvController controller) {
        controller.shutdown();
    }

    public static void shutdown(final ControllerManager controllerManager) {
        controllerManager.shutdown();
    }

    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Name server config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config items");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

    public static Properties getProperties() {
        return properties;
    }
}
