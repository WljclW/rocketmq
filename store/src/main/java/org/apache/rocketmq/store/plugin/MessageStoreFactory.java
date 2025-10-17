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

package org.apache.rocketmq.store.plugin;

import java.io.IOException;
import java.lang.reflect.Constructor;
import org.apache.rocketmq.store.MessageStore;

public final class MessageStoreFactory {
    /**
     * 【】：根据MessageStorePluginContext和MessageStore来构建出最终的MessageStore。。
     * [使用方法]：在brokerConfig的“messageStorePlugIn”属性值设置为“class1的全类名,class2的全类名”这样的形式；
     *        并且由于for循环的最后一步是“messageStore = pluginMessageStore;”，因此很多个class构造的时候类似
     *        于链式调用，后一个for循环创建的时候会用到前一个创建的结果———— 本质是一个 装饰器模式（Decorator Pattern），把
     *        原始的 defaultMessageStore 一层层包装。
     *        ————扩展点的使用，比如：可以实现自己的 MessageStoreFactoryPlugin 来扩展功能。
     * */
    public static MessageStore build(MessageStorePluginContext context,
        MessageStore messageStore) throws IOException {
        String plugin = context.getBrokerConfig().getMessageStorePlugIn();
        /*如果有实质性的内容，则会进入if块构建最终的MessageStore;否则会直接来到最后一行的return，返回参数传进来的MessageStore*/
        if (plugin != null && plugin.trim().length() != 0) {
            String[] pluginClasses = plugin.split(",");
            for (int i = pluginClasses.length - 1; i >= 0; --i) { //依次遍历并加载plugin串包含的所有类名，加载指定的类并利用反射创建对象
                String pluginClass = pluginClasses[i];
                try {
                    @SuppressWarnings("unchecked")
                    Class<AbstractPluginMessageStore> clazz = (Class<AbstractPluginMessageStore>) Class.forName(pluginClass);
                    Constructor<AbstractPluginMessageStore> construct = clazz.getConstructor(MessageStorePluginContext.class, MessageStore.class);
                    AbstractPluginMessageStore pluginMessageStore = construct.newInstance(context, messageStore);
                    messageStore = pluginMessageStore; //更新messageStore对象为将创建的对象。
                } catch (Throwable e) {
                    throw new RuntimeException("Initialize plugin's class: " + pluginClass + " not found!", e);
                }
            }
        }
        return messageStore;
    }
}
