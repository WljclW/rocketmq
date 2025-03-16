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

package org.apache.rocketmq.store.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.body.HARuntimeInfo;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;

/**RocketMQ高可用的实现原理:
     1）主服务器启动，并在特定端口上监听从服务器的连接。
     2）从服务器主动连接主服务器，主服务器接收客户端的连接，并
        建立相关TCP连接。
    3）从服务器主动向主服务器发送待拉取消息的偏移量，主服务器
        解析请求并返回消息给从服务器。
    4）从服务器保存消息并继续发送新的消息同步请求*/
public class DefaultHAService implements HAService {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    protected final AtomicInteger connectionCount = new AtomicInteger(0);

    protected final List<HAConnection> connectionList = new LinkedList<>();
    /*实现主 监听 从 的连接*/
    protected AcceptSocketService acceptSocketService;

    protected DefaultMessageStore defaultMessageStore;

    protected WaitNotifyObject waitNotifyObject = new WaitNotifyObject();
    protected AtomicLong push2SlaveMaxOffset = new AtomicLong(0);

    protected GroupTransferService groupTransferService;

    protected HAClient haClient;

    protected HAConnectionStateNotificationService haConnectionStateNotificationService;

    public DefaultHAService() {
    }

    @Override
    public void init(final DefaultMessageStore defaultMessageStore) throws IOException {
        this.defaultMessageStore = defaultMessageStore;
        this.acceptSocketService = new DefaultAcceptSocketService(defaultMessageStore.getMessageStoreConfig());
        this.groupTransferService = new GroupTransferService(this, defaultMessageStore);
        if (this.defaultMessageStore.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE) {
            this.haClient = new DefaultHAClient(this.defaultMessageStore);
        }
        this.haConnectionStateNotificationService = new HAConnectionStateNotificationService(this, defaultMessageStore);
    }

    @Override
    public void updateMasterAddress(final String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateMasterAddress(newAddr);
        }
    }

    @Override
    public void updateHaMasterAddress(String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateHaMasterAddress(newAddr);
        }
    }

    @Override
    public void putRequest(final CommitLog.GroupCommitRequest request) {
        this.groupTransferService.putRequest(request);
    }

    @Override
    public boolean isSlaveOK(final long masterPutWhere) {
        boolean result = this.connectionCount.get() > 0;
        result =
            result
                && masterPutWhere - this.push2SlaveMaxOffset.get() < this.defaultMessageStore
                .getMessageStoreConfig().getHaMaxGapNotInSync();
        return result;
    }

    public void notifyTransferSome(final long offset) {
        for (long value = this.push2SlaveMaxOffset.get(); offset > value; ) {
            boolean ok = this.push2SlaveMaxOffset.compareAndSet(value, offset);
            if (ok) {
                this.groupTransferService.notifyTransferSome();
                break;
            } else {
                value = this.push2SlaveMaxOffset.get();
            }
        }
    }

    @Override
    public AtomicInteger getConnectionCount() {
        return connectionCount;
    }

    @Override
    public void start() throws Exception {
        /*开始监听从的连接请求*/
        this.acceptSocketService.beginAccept();
        /*开始处理连接请求，从而创建HAConnection*/
        this.acceptSocketService.start();
        this.groupTransferService.start();
        this.haConnectionStateNotificationService.start();
        if (haClient != null) {
            this.haClient.start();
        }
    }

    public void addConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.add(conn);
        }
    }

    public void removeConnection(final HAConnection conn) {
        this.haConnectionStateNotificationService.checkConnectionStateAndNotify(conn);
        synchronized (this.connectionList) {
            this.connectionList.remove(conn);
        }
    }

    @Override
    public void shutdown() {
        if (this.haClient != null) {
            this.haClient.shutdown();
        }
        this.acceptSocketService.shutdown(true);
        this.destroyConnections();
        this.groupTransferService.shutdown();
        this.haConnectionStateNotificationService.shutdown();
    }

    public void destroyConnections() {
        synchronized (this.connectionList) {
            for (HAConnection c : this.connectionList) {
                c.shutdown();
            }

            this.connectionList.clear();
        }
    }

    public DefaultMessageStore getDefaultMessageStore() {
        return defaultMessageStore;
    }

    @Override
    public WaitNotifyObject getWaitNotifyObject() {
        return waitNotifyObject;
    }

    @Override
    public AtomicLong getPush2SlaveMaxOffset() {
        return push2SlaveMaxOffset;
    }

    @Override
    public int inSyncReplicasNums(final long masterPutWhere) {
        int inSyncNums = 1;
        for (HAConnection conn : this.connectionList) {
            if (this.isInSyncSlave(masterPutWhere, conn)) {
                inSyncNums++;
            }
        }
        return inSyncNums;
    }

    protected boolean isInSyncSlave(final long masterPutWhere, HAConnection conn) {
        if (masterPutWhere - conn.getSlaveAckOffset() < this.defaultMessageStore.getMessageStoreConfig()
            .getHaMaxGapNotInSync()) {
            return true;
        }
        return false;
    }

    @Override
    public void putGroupConnectionStateRequest(HAConnectionStateNotificationRequest request) {
        this.haConnectionStateNotificationService.setRequest(request);
    }

    @Override
    public List<HAConnection> getConnectionList() {
        return connectionList;
    }

    @Override
    public HAClient getHAClient() {
        return this.haClient;
    }

    @Override
    public HARuntimeInfo getRuntimeInfo(long masterPutWhere) {
        HARuntimeInfo info = new HARuntimeInfo();

        if (BrokerRole.SLAVE.equals(this.getDefaultMessageStore().getMessageStoreConfig().getBrokerRole())) {
            info.setMaster(false);

            info.getHaClientRuntimeInfo().setMasterAddr(this.haClient.getHaMasterAddress());
            info.getHaClientRuntimeInfo().setMaxOffset(this.getDefaultMessageStore().getMaxPhyOffset());
            info.getHaClientRuntimeInfo().setLastReadTimestamp(this.haClient.getLastReadTimestamp());
            info.getHaClientRuntimeInfo().setLastWriteTimestamp(this.haClient.getLastWriteTimestamp());
            info.getHaClientRuntimeInfo().setTransferredByteInSecond(this.haClient.getTransferredByteInSecond());
            info.getHaClientRuntimeInfo().setMasterFlushOffset(this.defaultMessageStore.getMasterFlushedOffset());
        } else {
            info.setMaster(true);
            int inSyncNums = 0;

            info.setMasterCommitLogMaxOffset(masterPutWhere);

            for (HAConnection conn : this.connectionList) {
                HARuntimeInfo.HAConnectionRuntimeInfo cInfo = new HARuntimeInfo.HAConnectionRuntimeInfo();

                long slaveAckOffset = conn.getSlaveAckOffset();
                cInfo.setSlaveAckOffset(slaveAckOffset);
                cInfo.setDiff(masterPutWhere - slaveAckOffset);
                cInfo.setAddr(conn.getClientAddress().substring(1));
                cInfo.setTransferredByteInSecond(conn.getTransferredByteInSecond());
                cInfo.setTransferFromWhere(conn.getTransferFromWhere());

                boolean isInSync = this.isInSyncSlave(masterPutWhere, conn);
                if (isInSync) {
                    inSyncNums++;
                }
                cInfo.setInSync(isInSync);

                info.getHaConnectionInfo().add(cInfo);
            }
            info.setInSyncSlaveNums(inSyncNums);
        }
        return info;
    }

    class DefaultAcceptSocketService extends AcceptSocketService {

        public DefaultAcceptSocketService(final MessageStoreConfig messageStoreConfig) {
            super(messageStoreConfig);
        }

        @Override
        protected HAConnection createConnection(SocketChannel sc) throws IOException {
            return new DefaultHAConnection(DefaultHAService.this, sc);
        }

        @Override
        public String getServiceName() {
            if (defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
                return defaultMessageStore.getBrokerConfig().getIdentifier() + AcceptSocketService.class.getSimpleName();
            }
            return DefaultAcceptSocketService.class.getSimpleName();
        }
    }

    /**
     * Listens to slave connections to create {@link HAConnection}.
     * []:AcceptSocketService实现主服务器监听从服务器的连接
     */
    protected abstract class AcceptSocketService extends ServiceThread {
        /*Broker服务器监听套接字（本地IP+端口号）*/
        private final SocketAddress socketAddressListen;
        /*服务端socket通道，基于NIO*/
        private ServerSocketChannel serverSocketChannel;
        /*时间选择器，基于NIO*/
        private Selector selector;

        private final MessageStoreConfig messageStoreConfig;

        public AcceptSocketService(final MessageStoreConfig messageStoreConfig) {
            this.messageStoreConfig = messageStoreConfig;
            this.socketAddressListen = new InetSocketAddress(messageStoreConfig.getHaListenPort());
        }

        /**
         * Starts listening to slave connections.
         * []:通过 ServerSocketChannel 和 Selector 设置了一个非阻塞的服务器端套接字，用于监听来自其他节点的连接请求
         *
         * @throws Exception If fails.
         */
        public void beginAccept() throws Exception {
            /*[作用]:调用open方法创建一个ServerSocketChannel实例。
            *什么是ServerSocketChannel？ Java NIO 提供的一种非阻塞式服务
            *       器套接字通道，用于接收客户端的连接请求*/
            this.serverSocketChannel = ServerSocketChannel.open();
            /*[]：调用 NetworkUtil.openSelector() 方法创建一个 Selector 实例。
            * 什么是Selector？
            *   Selector 是 Java NIO 的核心组件之一，用于监控多个通
            *   道的事件（如连接请求、读写操作等），从而实现高效的 I/O 多路复用。*/
            this.selector = NetworkUtil.openSelector();
            /*[]:调用 setReuseAddress(true) 方法设置套接字选项，允许重用本地地址和端口。
            * 这个选项通常用于避免因端口被占用而导致绑定失败的情况，特别是在服务器频繁重启时非常有用。
            * */
            this.serverSocketChannel.socket().setReuseAddress(true);
            /*[]:调用 bind() 方法将 ServerSocketChannel 绑定到指定的监听地址（socketAddressListen）
            *socketAddressListen 是一个 SocketAddress 对象，表示服务器需要监听的 IP 地址和端口号。
            * */
            this.serverSocketChannel.socket().bind(this.socketAddressListen);
            /*如果配置文件中未指定高可用性监听端口（即 haListenPort 为 0），则动态获取操作系统分配的端口号。
            调用 getLocalPort() 方法获取实际绑定的端口号，并将其设置到 messageStoreConfig 中。*/
            if (0 == messageStoreConfig.getHaListenPort()) {
                messageStoreConfig.setHaListenPort(this.serverSocketChannel.socket().getLocalPort());
                log.info("OS picked up {} to listen for HA", messageStoreConfig.getHaListenPort());
            }
            /*[]:调用 configureBlocking(false) 方法将 ServerSocketChannel 设置为非阻塞模式。
            在非阻塞模式下，accept() 方法不会阻塞线程，而是立即返回（如果没有连接请求）。
            * */
            this.serverSocketChannel.configureBlocking(false);
            /*将 ServerSocketChannel 注册到 Selector 上，并指定感兴趣的事件类型为 SelectionKey.OP_ACCEPT。
            OP_ACCEPT 表示 Selector 会监控该通道是否有新的连接请求到达。*/
            this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void shutdown(final boolean interrupt) {
            super.shutdown(interrupt);
            try {
                if (null != this.serverSocketChannel) {
                    this.serverSocketChannel.close();
                }

                if (null != this.selector) {
                    this.selector.close();
                }
            } catch (IOException e) {
                log.error("AcceptSocketService shutdown exception", e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");
            /*只要服务状态正常就不断执行*/
            while (!this.isStopped()) {
                try {
                    /**标准的基于NIO的服务端程序实例，选择器每1s处理一次连接事件。连接事件
                     * 就绪后，调用ServerSocketChannel的accept()方法创建SocketChannel。
                     * 然后为每一个连接创建一个HAConnection对象，该HAConnection将负责主
                     * 从数据同步逻辑。*/
                    this.selector.select(1000);
                    Set<SelectionKey> selected = this.selector.selectedKeys();

                    if (selected != null) {
                        for (SelectionKey k : selected) {
                            if (k.isAcceptable()) {
                                SocketChannel sc = ((ServerSocketChannel) k.channel()).accept();

                                if (sc != null) {
                                    DefaultHAService.log.info("HAService receive new connection, "
                                        + sc.socket().getRemoteSocketAddress());
                                    try {
                                        HAConnection conn = createConnection(sc);
                                        conn.start();
                                        DefaultHAService.this.addConnection(conn);
                                    } catch (Exception e) {
                                        log.error("new HAConnection exception", e);
                                        sc.close();
                                    }
                                }
                            } else {
                                log.warn("Unexpected ops in select " + k.readyOps());
                            }
                        }

                        selected.clear();
                    }
                } catch (Exception e) {
                    log.error(this.getServiceName() + " service has exception.", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        /**
         * Create ha connection
         */
        protected abstract HAConnection createConnection(final SocketChannel sc) throws IOException;
    }
}
