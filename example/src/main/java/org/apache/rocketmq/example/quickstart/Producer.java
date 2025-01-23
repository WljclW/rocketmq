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
package org.apache.rocketmq.example.quickstart;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * This class demonstrates how to send messages to brokers using provided {@link DefaultMQProducer}.
 */
public class Producer {

    /**
     * The number of produced messages.
     */
    public static final int MESSAGE_COUNT = 100;
    public static final String PRODUCER_GROUP = "please_rename_unique_group_name";
    public static final String DEFAULT_NAMESRVADDR = "localhost:9876";
    public static final String TOPIC = "TopicTest1119";
    public static final String TAG = "TagAB";
    public static final String KEY = "Debug_key";

    public static void main(String[] args) throws MQClientException, InterruptedException {

        /*
         * Instantiate with a producer group name.
         * 消息生产者的代码都在client模块中，相对于RocketMQ来说，它就是客户端，也是 消息 的提供者
         */
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);

        /*
         * Specify name server addresses.
         *
         * Alternatively, you may specify name server addresses via exporting environmental variable: NAMESRV_ADDR
         * <pre>
         * {@code
         *  producer.setNamesrvAddr("name-server1-ip:9876;name-server2-ip:9876");
         * }
         * </pre>
         */
        // Uncomment the following line while debugging, namesrvAddr should be set to your local address
        // producer.setNamesrvAddr(DEFAULT_NAMESRVADDR);

        /*
         * Launch the instance.
         */
        producer.setNamesrvAddr(DEFAULT_NAMESRVADDR);   //设置namesrv的地址
        producer.start();

        for (int i = 0; i < MESSAGE_COUNT; i++) {
            try {

                /*
                 * Create a message instance, specifying topic, tag and message body.
                 * 创建消息实例必须指定topic,body；除此以外还可以指定tag，key，flag，MessageConst.PROPERTY_WAIT_STORE_MSG_OK
                 */
                Message msg = new Message(TOPIC /* Topic */,
                    TAG /* Tag */,
                    KEY,
                    ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
                );

                /*
                 * Call send message to deliver message to one of brokers.
                 * 根据"send"的不同方法，可能有返回值，也可能没有返回值。
                 * 是同步发送的时候通常有返回值；sendOneway方法没有返回值(不关注返回值，只管发送)；是异步发送
                 *      的时候通常要设置回调。
                 */
                SendResult sendResult = producer.send(msg, 20 * 1000);
                /* 一、单向发送的例子
                 * There are different ways to send message, if you don't care about the send result,you can use this way
                 * {@code
                 * producer.sendOneway(msg);
                 * }
                 */

                /* 二、同步发送的例子
                 * if you want to get the send result in a synchronize way, you can use this send method
                 * {@code
                 * SendResult sendResult = producer.send(msg);
                 * System.out.printf("%s%n", sendResult);
                 * }
                 */

                /* 三、异步发送的例子，设置设置回调逻辑
                 * if you want to get the send result in a asynchronize way, you can use this send method
                 * {@code
                 *
                 *  producer.send(msg, new SendCallback() {
                 *  @Override
                 *  public void onSuccess(SendResult sendResult) {
                 *      // do something
                 *  }
                 *
                 *  @Override
                 *  public void onException(Throwable e) {
                 *      // do something
                 *  }
                 *});
                 *
                 *}
                 */

                System.out.printf("%s%n", sendResult);
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }

        /*
         * Shut down once the producer instance is no longer in use.
         */
        producer.shutdown();
    }
}


/**
 * 同步： 发送者向MQ执行发送消息API时，同步等待， 直到消息服务器返回发送结果。
 *
 * 异步： 发送者向MQ执行发送消息API时，指定消息发送成功后的回掉函数，然后调 用消息发送API后，立即返回，消
 *      息发送者线程不阻塞，直到运行结束，消息发送成功或 失败的回调任务在一个新的线程中执行。
 *
 * 单向：消息发送者向MQ执行发送消息API时，直接返回，不等待消息服务器的结果， 也不注册回调函数，简单地说，
 *      就是只管发，不在乎消息是否成功存储在消息服务器上。
 *
 * */
