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

/**
 *【作用】在原生线程的基础上添加了线程状态感知
 *【说名】
 *      1. 以前的线程方法存在的问题————
 *              ❌ 问题：Java 原生 Thread.start() 是异步的
 *              thread.start();  // 立即返回
 *           此时 thread 可能还在初始化，run() 还没执行！如果你立即调用 thread.isAlive()，虽然返回 true，但：
 *          它可能还没进入 run() 方法，某些资源尚未初始化其他线程依赖它的状态就会出错
 */
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
     *      wait() 和 notify() 必须配合对象监视器锁使用。也就是说，started.wait() 只能在持有 started 这个对象的锁时调用，否则会抛 IllegalMonitorStateException。
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
