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

package org.apache.rocketmq.tools.command.stats;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.stats.Stats;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.body.BrokerStatsData;
import org.apache.rocketmq.remoting.protocol.body.GroupList;
import org.apache.rocketmq.remoting.protocol.body.TopicList;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

/**[]:RocketMQ 提供的一个命令行工具类，用于统计和分析 RocketMQ 集群的整体运行状态。它是 mqadmin 工具的一部
 *      分，允许用户通过命令行获取集群、主题（Topic）、消费者组（Consumer Group）等的详细统计信息。
 * [其他说明]
 *    1. 这个类在执行时，其实主要的是利用到了DefaultMQAdminExt类提供的多个方法。因此可以看出：运维相关的命令
 *      可以使用DefaultMQAdminExt类提供的多个方法，将它们组合使用，实现特定的需求*/
public class StatsAllSubCommand implements SubCommand {
    public static void printTopicDetail(final DefaultMQAdminExt admin, final String topic, final boolean activeTopic)
        throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        TopicRouteData topicRouteData = admin.examineTopicRouteInfo(topic);

        GroupList groupList = admin.queryTopicConsumeByWho(topic);

        double inTPS = 0;

        long inMsgCntToday = 0;

        for (BrokerData bd : topicRouteData.getBrokerDatas()) {
            String masterAddr = bd.getBrokerAddrs().get(MixAll.MASTER_ID);
            if (masterAddr != null) {
                try {
                    BrokerStatsData bsd = admin.viewBrokerStatsData(masterAddr, Stats.TOPIC_PUT_NUMS, topic);
                    inTPS += bsd.getStatsMinute().getTps();
                    inMsgCntToday += compute24HourSum(bsd);
                } catch (Exception e) {
                }
            }
        }

        if (groupList != null && !groupList.getGroupList().isEmpty()) {

            for (String group : groupList.getGroupList()) {
                double outTPS = 0;
                long outMsgCntToday = 0;

                for (BrokerData bd : topicRouteData.getBrokerDatas()) {
                    String masterAddr = bd.getBrokerAddrs().get(MixAll.MASTER_ID);
                    if (masterAddr != null) {
                        try {
                            String statsKey = String.format("%s@%s", topic, group);
                            BrokerStatsData bsd = admin.viewBrokerStatsData(masterAddr, Stats.GROUP_GET_NUMS, statsKey);
                            outTPS += bsd.getStatsMinute().getTps();
                            outMsgCntToday += compute24HourSum(bsd);
                        } catch (Exception e) {
                        }
                    }
                }

                long accumulate = 0;
                try {
                    ConsumeStats consumeStats = admin.examineConsumeStats(group, topic);
                    if (consumeStats != null) {
                        accumulate = consumeStats.computeTotalDiff();
                        if (accumulate < 0) {
                            accumulate = 0;
                        }
                    }
                } catch (Exception e) {
                }

                if (!activeTopic || inMsgCntToday > 0 ||
                    outMsgCntToday > 0) {

                    System.out.printf("%-64s  %-64s %12d %11.2f %11.2f %14d %14d%n",
                        UtilAll.frontStringAtLeast(topic, 64),
                        UtilAll.frontStringAtLeast(group, 64),
                        accumulate,
                        inTPS,
                        outTPS,
                        inMsgCntToday,
                        outMsgCntToday
                    );
                }
            }
        } else {
            if (!activeTopic || inMsgCntToday > 0) {

                System.out.printf("%-64s  %-64s %12d %11.2f %11s %14d %14s%n",
                    UtilAll.frontStringAtLeast(topic, 64),
                    "",
                    0,
                    inTPS,
                    "",
                    inMsgCntToday,
                    "NO_CONSUMER"
                );
            }
        }
    }

    public static long compute24HourSum(BrokerStatsData bsd) {
        if (bsd.getStatsDay().getSum() != 0) {
            return bsd.getStatsDay().getSum();
        }

        if (bsd.getStatsHour().getSum() != 0) {
            return bsd.getStatsHour().getSum();
        }

        if (bsd.getStatsMinute().getSum() != 0) {
            return bsd.getStatsMinute().getSum();
        }

        return 0;
    }

    @Override
    public String commandName() {
        return "statsAll";
    }

    @Override
    public String commandDesc() {
        return "Topic and Consumer tps stats.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("a", "activeTopic", false, "print active topic only");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "print select topic only");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        /**step1:构建defaultMQAdminExt对象，将对象的名字设置为当前的时间戳。。。rocketmq原生支持的命令都是基于这个对象*/
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();
            /**step2：获取所有的topic*/
            TopicList topicList = defaultMQAdminExt.fetchAllTopicList();

            System.out.printf("%-64s  %-64s %12s %11s %11s %14s %14s%n",
                "#Topic",
                "#Consumer Group",
                "#Accumulation",
                "#InTPS",
                "#OutTPS",
                "#InMsg24Hour",
                "#OutMsg24Hour"
            );

            boolean activeTopic = commandLine.hasOption('a');
            String selectTopic = commandLine.getOptionValue('t');
            /**step3：遍历topic的路由信息，向所有的Broker查看关于该topic的统计信息，具体由DefaultMQAdminExt的
             viewBrokerStatsData()方法实现*/
            for (String topic : topicList.getTopicList()) {
                if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX) || topic.startsWith(MixAll.DLQ_GROUP_TOPIC_PREFIX)) {
                    continue;
                }

                if (selectTopic != null && !selectTopic.isEmpty() && !topic.equals(selectTopic)) {
                    continue;
                }

                try {
                    printTopicDetail(defaultMQAdminExt, topic, activeTopic);
                } catch (Exception e) {
                }
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
