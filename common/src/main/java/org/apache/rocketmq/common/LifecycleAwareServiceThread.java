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
package org.apache.rocketmq.common;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class LifecycleAwareServiceThread extends ServiceThread {

    private final AtomicBoolean started = new AtomicBoolean(false);

    /**
     *    【总述】这个run方法提供了一种更通用、功能更完全的实现。其中"started.notifyAll();"会唤醒所有等在started对象锁的线程，
     * 而服务真正的逻辑在run0方法实现。
                比如：下面的代码段就是用于等待获取started对象锁
                     // 某个其他线程
                     synchronized (started) {
                         while (!started.get()) {
                            started.wait(); // 等待服务启动
                         }
                     // 服务已经启动，继续干活
                     }
     * */
    @Override
    public void run() {
        started.set(true);
        synchronized (started) {
            started.notifyAll();
        }

        run0();
    }

    public abstract void run0();

    /**
     * Take spurious wakeup into account.
     *
     * @param timeout amount of time in milliseconds
     * @throws InterruptedException if interrupted
     */
    public void awaitStarted(long timeout) throws InterruptedException {
        long expire = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
        synchronized (started) {
            while (!started.get()) {
                long duration = expire - System.nanoTime();
                if (duration < TimeUnit.MILLISECONDS.toNanos(1)) {
                    break;
                }
                started.wait(TimeUnit.NANOSECONDS.toMillis(duration));
            }
        }
    }
}
