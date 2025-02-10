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

    private final static Logger log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);
    private final static Logger logConsole = LoggerFactory.getLogger(LoggerName.NAMESRV_CONSOLE_LOGGER_NAME);
    private static Properties properties = null;
    private static NamesrvConfig namesrvConfig = null;
    private static NettyServerConfig nettyServerConfig = null;
    private static NettyClientConfig nettyClientConfig = null;
    private static ControllerConfig controllerConfig = null;

    public static void main(String[] args) {
        main0(args);    //启动namesrvController
        controllerManagerMain();   //启动ControllerManager
    }

    public static NamesrvController main0(String[] args) {
        try {
            parseCommandlineAndConfigFile(args); //解析命令行和配置文件，最后达成：为NamesrvConfig、NettyServerConfig、NettyClientConfig属性设置值
            /**
             * 创建并启动 NamesrvController 对象，该对象是namesrv的核
             * 心控制器，它持有各种配置对象、网络通信对象、路由管理对象等
             * */
            NamesrvController controller = createAndStartNamesrvController();
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
     * 【主要目的】填充NamesevConfig(NameServer业务参数)、NettyServerConfig(NameServer网络参数)、
     *      NettyClientConfig属性值
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
        // 会添加 -c,-p 参数
        CommandLine commandLine = ServerUtil.parseCmdLine("mqnamesrv", args, buildCommandlineOptions(options), new DefaultParser());
        if (null == commandLine) {
            System.exit(-1);
            return;
        }
        //下面的三行初始化配置对象
        namesrvConfig = new NamesrvConfig();
        nettyServerConfig = new NettyServerConfig();
        nettyClientConfig = new NettyClientConfig();
        nettyServerConfig.setListenPort(9876);  // 设置Netty服务器的监听端口  也就是默认的 NameServer 端口是 9876
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
                if (namesrvConfig.isEnableControllerInNamesrv()) {      // 如果配置中启用了控制器功能，初始化并配置控制器
                    controllerConfig = new ControllerConfig();
                    JraftConfig jraftConfig = new JraftConfig();
                    controllerConfig.setJraftConfig(jraftConfig);
                    MixAll.properties2Object(properties, controllerConfig);
                    MixAll.properties2Object(properties, jraftConfig);
                }
                namesrvConfig.setConfigStorePath(file);     //设置配置文件的路径

                System.out.printf("load config properties file OK, %s%n", file);
                in.close();
            }
        }
        //将命令行选项的完整名称（eg：-n-->namesrvAddr）对应的选项值设置到NameSrvConfig的对应属性上
        MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), namesrvConfig);
        //-p选项可以将所有的属性条目打印在控制台，借助printObjectProperties方法进行打印
        if (commandLine.hasOption('p')) {
            MixAll.printObjectProperties(logConsole, namesrvConfig);
            MixAll.printObjectProperties(logConsole, nettyServerConfig);
            MixAll.printObjectProperties(logConsole, nettyClientConfig);
            if (namesrvConfig.isEnableControllerInNamesrv()) {      //同样如果配置文件设置了raft配置，也需要打印出关于它的配置
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

        NamesrvController controller = createNamesrvController();   //创建namesrv控制器实例，在创建过程中做了一些事
        start(controller);  //启动上一步创建好的NamesrvController
        NettyServerConfig serverConfig = controller.getNettyServerConfig();
        String tip = String.format("The Name Server boot success. serializeType=%s, address %s:%d", RemotingCommand.getSerializeTypeConfigInThisServer(), serverConfig.getBindAddress(), serverConfig.getListenPort());
        log.info(tip);  //将上面格式化后的提示信息tip记录在日志文件
        System.out.printf("%s%n", tip);
        return controller;
    }

    /**
     * 创建namesrvController的流程信息：
     * step1:创建一个NamesrvController对象，传入namesrvConfig、nettyServerConfig和nettyClientConfig对象，这
     *      些对象存储了namesrv的业务配置和网络配置
     * step2:调用NamesrvController对象的getConfiguration方法，获取一个Configuration对象，该对象负责管理namesrv
     *      的配置信息
     * step3:调用Configuration对象的registerConfig方法，传入properties对象，将properties对象中的配置信息注册
     *      到Configuration对象中
     * step4:返回NamesrvController对象
     * */
    public static NamesrvController createNamesrvController() {
        // 创建 NamesrvController，传入namesrvConfig、nettyServerConfig、nettyClientConfig配置
        final NamesrvController controller = new NamesrvController(namesrvConfig, nettyServerConfig, nettyClientConfig);
        // remember all configs to prevent discard
        controller.getConfiguration().registerConfig(properties);
        return controller;
    }

    public static NamesrvController start(final NamesrvController controller) throws Exception {

        if (null == controller) {
            throw new IllegalArgumentException("NamesrvController is null");
        }
        //初始化NamesrvController
        boolean initResult = controller.initialize();
        if (!initResult) {
            controller.shutdown();
            System.exit(-3);
        }

        // 如何理解jvm钩子函数：当jvm关闭的时候，会执行系统中已经设置的所有通过方
        //      法addShutdownHook添加的钩子，当系统执行完这些钩子后，jvm才会关闭。所以
        //      这些钩子可以在jvm关闭的时候进行内存清理、对象销毁等操作。
        //通过addShutdownHook注册jvm钩子函数，当程序关闭时，会调用controller的shutdown方法
        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(log, (Callable<Void>) () -> {
            controller.shutdown();
            return null;
        }));

        controller.start();

        return controller;
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

    /**
     * 重载的方法start。用于启动ControllerManager，逻辑上和start(final NamesrvController controller)是一样的，即————
     *      1.先是进行初始化方法的调用
     *      2.如果初始化是没有成功就直接返回
     *      3.到这里说明初始化成功了。这一步会注册jvm钩子函数实现优雅关闭
     *      4.调用ControllerManager的start方法启动controllerManager
     * */
    public static ControllerManager start(final ControllerManager controllerManager) throws Exception {

        if (null == controllerManager) {
            throw new IllegalArgumentException("ControllerManager is null");
        }
        //调用initialize方法初始化controllerManager
        boolean initResult = controllerManager.initialize();
        if (!initResult) {
            controllerManager.shutdown();
            System.exit(-3);
        }
        //注册jvm钩子函数，当程序关闭时，会调用controllerManager的shutdown方法
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

    //这两个方法其实继续添加了options元素。其中一个是 namesrv的配置文件，一个是 打印所有的config条目
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
