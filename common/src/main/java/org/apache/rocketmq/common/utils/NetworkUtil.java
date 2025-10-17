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
package org.apache.rocketmq.common.utils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayList;
import java.util.Enumeration;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class NetworkUtil {
    public static final String OS_NAME = System.getProperty("os.name");

    private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);
    private static boolean isLinuxPlatform = false;
    private static boolean isWindowsPlatform = false;

    static {
        if (OS_NAME != null && OS_NAME.toLowerCase().contains("linux")) {
            isLinuxPlatform = true;
        }

        if (OS_NAME != null && OS_NAME.toLowerCase().contains("windows")) {
            isWindowsPlatform = true;
        }
    }

    public static boolean isWindowsPlatform() {
        return isWindowsPlatform;
    }

    public static Selector openSelector() throws IOException {
        Selector result = null;

        if (isLinuxPlatform()) {
            try {
                final Class<?> providerClazz = Class.forName("sun.nio.ch.EPollSelectorProvider");
                if (providerClazz != null) {
                    try {
                        final Method method = providerClazz.getMethod("provider");
                        if (method != null) {
                            final SelectorProvider selectorProvider = (SelectorProvider) method.invoke(null);
                            if (selectorProvider != null) {
                                result = selectorProvider.openSelector();
                            }
                        }
                    } catch (final Exception e) {
                        log.warn("Open ePoll Selector for linux platform exception", e);
                    }
                }
            } catch (final Exception e) {
                // ignore
            }
        }

        if (result == null) {
            result = Selector.open();
        }

        return result;
    }

    public static boolean isLinuxPlatform() {
        return isLinuxPlatform;
    }

    /**
     *[aim]用来获取当前机器可用 IP 地址的核心工具方法，它在 Broker、NameServer、Producer、Consumer 启动时
     *      被调用，用于确定本机的网络标识（IP），以便注册到集群、建立连接或对外提供服务
     *[步骤]该方法通过遍历所有网络接口，排除无效地址（如回环、私有、虚拟网卡），优先选择非 192.168.x.x/127.0.0.1
     *      的公网或局域网 IP，尽可能返回一个“可被其他节点访问”的真实 IP 地址。
     * @return
     */
    public static String getLocalAddress() {
        try {
            // Traversal Network interface to get the first non-loopback and non-private address
            // 获取系统中所有网卡（如 eth0, lo, docker0, vethxxx 等）
            Enumeration<NetworkInterface> enumeration = NetworkInterface.getNetworkInterfaces();
            ArrayList<String> ipv4Result = new ArrayList<>();
            ArrayList<String> ipv6Result = new ArrayList<>();
            while (enumeration.hasMoreElements()) {
                final NetworkInterface nif = enumeration.nextElement();
                //过滤无效网卡
                if (isBridge(nif) || nif.isVirtual() || nif.isPointToPoint() || !nif.isUp()) {
                    continue;
                }

                //遍历网卡的每一个ip地址。排除回环地址：!address.isLoopbackAddress() → 跳过 127.0.0.1、::1；按照协议分类加入ArrayList
                final Enumeration<InetAddress> en = nif.getInetAddresses();
                while (en.hasMoreElements()) {
                    final InetAddress address = en.nextElement();
                    if (!address.isLoopbackAddress()) {
                        if (address instanceof Inet6Address) {
                            ipv6Result.add(normalizeHostAddress(address));
                        } else {
                            ipv4Result.add(normalizeHostAddress(address));
                        }
                    }
                }
            }

            // prefer ipv4（优先选择 IPv4，并排除常见私有地址）
            if (!ipv4Result.isEmpty()) {
                for (String ip : ipv4Result) {
                    if (ip.startsWith("127.0") || ip.startsWith("192.168") || ip.startsWith("0.")) {
                        continue;
                    }
                    //返回第一个非私有、非回环的 IPv4 地址（如 10.10.10.10 或公网 IP）
                    return ip;
                }

                return ipv4Result.get(ipv4Result.size() - 1);
            } else if (!ipv6Result.isEmpty()) {
                return ipv6Result.get(0);
            }
            //If failed to find,fall back to localhost
            //如果前面都失败，使用 JDK 默认方式获取主机地址；可能返回 127.0.0.1 或主机名，不够可靠
            final InetAddress localHost = InetAddress.getLocalHost();
            return normalizeHostAddress(localHost);
        } catch (Exception e) {
            log.error("Failed to obtain local address", e);
        }

        return null;
    }

    public static String normalizeHostAddress(final InetAddress localHost) {
        if (localHost instanceof Inet6Address) {
            return "[" + localHost.getHostAddress() + "]";
        } else {
            return localHost.getHostAddress();
        }
    }

    /**根据“主机：端口号”生成InetSocketAddress*/
    public static SocketAddress string2SocketAddress(final String addr) {
        int split = addr.lastIndexOf(":"); //实现对于ipv6的支持
        String host = addr.substring(0, split);
        String port = addr.substring(split + 1);
        InetSocketAddress isa = new InetSocketAddress(host, Integer.parseInt(port));
        return isa;
    }

    public static String socketAddress2String(final SocketAddress addr) {
        StringBuilder sb = new StringBuilder();
        InetSocketAddress inetSocketAddress = (InetSocketAddress) addr;
        sb.append(inetSocketAddress.getAddress().getHostAddress());
        sb.append(":");
        sb.append(inetSocketAddress.getPort());
        return sb.toString();
    }

    public static String convert2IpString(final String addr) {
        return socketAddress2String(string2SocketAddress(addr));
    }

    private static boolean isBridge(NetworkInterface networkInterface) {
        try {
            if (isLinuxPlatform()) {
                String interfaceName = networkInterface.getName();
                File file = new File("/sys/class/net/" + interfaceName + "/bridge");
                return file.exists();
            }
        } catch (SecurityException e) {
            //Ignore
        }
        return false;
    }
}
