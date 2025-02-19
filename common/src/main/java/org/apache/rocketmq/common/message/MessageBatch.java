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
package org.apache.rocketmq.common.message;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.rocketmq.common.MixAll;

/**
 * 批量消息的抽象，本质就是内部持有List的消息集合
 * */
public class MessageBatch extends Message implements Iterable<Message> {

    private static final long serialVersionUID = 621335151046335557L;
    private final List<Message> messages;

    public MessageBatch(List<Message> messages) {
        this.messages = messages;
    }

    public byte[] encode() {
        return MessageDecoder.encodeMessages(messages);
    }

    public Iterator<Message> iterator() {
        return messages.iterator();
    }

    /**
     * 1.这个方法会根据集合中的消息来生成批量消息。。但是会进行一些检查：
     *      1.如果是延迟消息则不支持（判断：延迟等级大于0）
     *      2.如果是重试消息则不支持（判断：topic是否以RETRY_GROUP_TOPIC_PREFIX开头）
     *      3.如果批量消息中的topic不一致则不支持（思路：记录下第一个消息并判断后续的消息的主题）
     * 2.从这个方法可以看出，MessageBatch就是内部持有List<Message>，这样发送多条消息就和发送单条
     *      消息的逻辑上保持一致了
     * */
    public static MessageBatch generateFromList(Collection<? extends Message> messages) {
        assert messages != null;
        assert messages.size() > 0;
        List<Message> messageList = new ArrayList<>(messages.size());
        Message first = null;
        for (Message message : messages) {
            if (message.getDelayTimeLevel() > 0) {
                throw new UnsupportedOperationException("TimeDelayLevel is not supported for batching");
            }
            if (message.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                throw new UnsupportedOperationException("Retry Group is not supported for batching");
            }
            if (first == null) {
                first = message;
            } else {
                if (!first.getTopic().equals(message.getTopic())) {
                    throw new UnsupportedOperationException("The topic of the messages in one batch should be the same");
                }
                if (first.isWaitStoreMsgOK() != message.isWaitStoreMsgOK()) {
                    throw new UnsupportedOperationException("The waitStoreMsgOK of the messages in one batch should the same");
                }
            }
            messageList.add(message);
        }
        MessageBatch messageBatch = new MessageBatch(messageList);

        messageBatch.setTopic(first.getTopic());
        messageBatch.setWaitStoreMsgOK(first.isWaitStoreMsgOK());
        return messageBatch;
    }

}
