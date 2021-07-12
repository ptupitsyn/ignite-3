/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.client.internal.io.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.ignite.client.ClientMessageDecoder;
import org.apache.ignite.client.IgniteClientConfiguration;
import org.apache.ignite.client.IgniteClientConnectionException;
import org.apache.ignite.client.internal.io.ClientConnection;
import org.apache.ignite.client.internal.io.ClientConnectionMultiplexer;
import org.apache.ignite.client.internal.io.ClientConnectionStateHandler;
import org.apache.ignite.client.internal.io.ClientMessageHandler;

import java.net.InetSocketAddress;

public class NettyClientConnectionMultiplexer implements ClientConnectionMultiplexer {
    /** */
    private final IgniteClientConfiguration clientCfg;

    /** */
    private final NioEventLoopGroup workerGroup;

    /** */
    private final Bootstrap bootstrap;

    public NettyClientConnectionMultiplexer(IgniteClientConfiguration clientCfg) {
        this.clientCfg = clientCfg;
        workerGroup = new NioEventLoopGroup();
        bootstrap = new Bootstrap();
    }

    @Override public void start() {
        // TODO: Is this method needed?
        try {
            bootstrap.group(workerGroup);
            bootstrap.channel(NioSocketChannel.class);
            bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
            bootstrap.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch)
                        throws Exception {
                    ch.pipeline().addLast(
                            new ClientMessageDecoder(),
                            new NettyClientMessageHandler());
                }
            });

        } catch (Throwable t){
            workerGroup.shutdownGracefully();

            throw t;
        }
    }

    @Override public void stop() {
        workerGroup.shutdownGracefully();
    }

    @Override public ClientConnection open(InetSocketAddress addr,
                                           ClientMessageHandler msgHnd,
                                           ClientConnectionStateHandler stateHnd)
            throws IgniteClientConnectionException, InterruptedException {

        // TODO: Make this method async.
        ChannelFuture f = bootstrap.connect(addr).sync();

        return new NettyClientConnection(f.channel(), msgHnd, stateHnd);
    }
}
