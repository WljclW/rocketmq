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
package org.apache.rocketmq.common.sysflag;

public class PullSysFlag {
    /*分别是1左移不同的位数*/
    private final static int FLAG_COMMIT_OFFSET = 0x1; // 00000001，是否需要提交消费进度
    private final static int FLAG_SUSPEND = 0x1 << 1; // 00000010，表示是否挂起拉取请求
    private final static int FLAG_SUBSCRIPTION = 0x1 << 2; // 00001000，表示是否包含订阅关系(表示消费者拉取消息时会携带订阅的数据)
    private final static int FLAG_CLASS_FILTER = 0x1 << 3; // 00010000，表示是否包含类过滤规则
    private final static int FLAG_LITE_PULL_MESSAGE = 0x1 << 4; // 00100000，表示是否是轻量拉取消息

    public static int buildSysFlag(final boolean commitOffset, final boolean suspend,
        final boolean subscription, final boolean classFilter) {
        int flag = 0;

        if (commitOffset) {
            flag |= FLAG_COMMIT_OFFSET;
        }

        if (suspend) {
            flag |= FLAG_SUSPEND;
        }

        if (subscription) {
            flag |= FLAG_SUBSCRIPTION;
        }

        if (classFilter) {
            flag |= FLAG_CLASS_FILTER;
        }

        return flag;
    }

    public static int buildSysFlag(final boolean commitOffset, final boolean suspend,
        final boolean subscription, final boolean classFilter, final boolean litePull) {
        int flag = buildSysFlag(commitOffset, suspend, subscription, classFilter);

        if (litePull) {
            flag |= FLAG_LITE_PULL_MESSAGE;
        }

        return flag;
    }

    /**清除某一个标志的方法"与该标志的非 求与"*/
    public static int clearCommitOffsetFlag(final int sysFlag) {
        return sysFlag & (~FLAG_COMMIT_OFFSET);
    }

    public static boolean hasCommitOffsetFlag(final int sysFlag) {
        return (sysFlag & FLAG_COMMIT_OFFSET) == FLAG_COMMIT_OFFSET;
    }

    public static boolean hasSuspendFlag(final int sysFlag) {
        return (sysFlag & FLAG_SUSPEND) == FLAG_SUSPEND;
    }

    public static int clearSuspendFlag(final int sysFlag) {
        return sysFlag & (~FLAG_SUSPEND);
    }

    public static boolean hasSubscriptionFlag(final int sysFlag) {
        return (sysFlag & FLAG_SUBSCRIPTION) == FLAG_SUBSCRIPTION;
    }

    public static int buildSysFlagWithSubscription(final int sysFlag) {
        return sysFlag | FLAG_SUBSCRIPTION;
    }

    public static boolean hasClassFilterFlag(final int sysFlag) {
        return (sysFlag & FLAG_CLASS_FILTER) == FLAG_CLASS_FILTER;
    }

    public static boolean hasLitePullFlag(final int sysFlag) {
        return (sysFlag & FLAG_LITE_PULL_MESSAGE) == FLAG_LITE_PULL_MESSAGE;
    }
}
