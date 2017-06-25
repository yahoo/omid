/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.omid.tso;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.omid.metrics.MetricsRegistry;
import org.apache.omid.proto.TSOProto;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.Executors;

/**
 * ChannelHandler for the TSO Server.
 *
 * Incoming requests are processed in this class
 */
public class TSOChannelHandler extends SimpleChannelHandler implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(TSOChannelHandler.class);

    private final ChannelFactory factory;

    private final ServerBootstrap bootstrap;

    @VisibleForTesting
    Channel listeningChannel;
    @VisibleForTesting
    ChannelGroup channelGroup;

    private RequestProcessor requestProcessor;

    private TSOServerConfig config;

    private MetricsRegistry metrics;

    @Inject
    public TSOChannelHandler(TSOServerConfig config, RequestProcessor requestProcessor, MetricsRegistry metrics) {

        this.config = config;
        this.metrics = metrics;
        this.requestProcessor = requestProcessor;
        // Setup netty listener
        this.factory = new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("boss-%d").build()),
                Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("worker-%d").build()),
                (Runtime.getRuntime().availableProcessors() * 2 + 1) * 2);

        this.bootstrap = new ServerBootstrap(factory);
        bootstrap.setPipelineFactory(new TSOPipelineFactory(this));

    }

    /**
     * Allows to create and connect the communication channel closing the previous one if existed
     */
    void reconnect() {
        if (listeningChannel == null && channelGroup == null) {
            LOG.debug("Creating communication channel...");
        } else {
            LOG.debug("Reconnecting communication channel...");
            closeConnection();
        }
        // Create the global ChannelGroup
        channelGroup = new DefaultChannelGroup(TSOChannelHandler.class.getName());
        LOG.debug("\tCreating channel to listening for incoming connections in port {}", config.getPort());
        listeningChannel = bootstrap.bind(new InetSocketAddress(config.getPort()));
        channelGroup.add(listeningChannel);
        LOG.debug("\tListening channel created and connected: {}", listeningChannel);
    }

    /**
     * Allows to close the communication channel
     */
    void closeConnection() {
        LOG.debug("Closing communication channel...");
        if (listeningChannel != null) {
            LOG.debug("\tUnbinding listening channel {}", listeningChannel);
            listeningChannel.unbind().awaitUninterruptibly();
            LOG.debug("\tListening channel {} unbound", listeningChannel);
        }
        if (channelGroup != null) {
            LOG.debug("\tClosing channel group {}", channelGroup);
            channelGroup.close().awaitUninterruptibly();
            LOG.debug("\tChannel group {} closed", channelGroup);
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Netty SimpleChannelHandler implementation
    // ----------------------------------------------------------------------------------------------------------------

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        channelGroup.add(ctx.getChannel());
        LOG.debug("TSO channel connected: {}", ctx.getChannel());
    }

    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        channelGroup.remove(ctx.getChannel());
        LOG.debug("TSO channel disconnected: {}", ctx.getChannel());
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        LOG.debug("TSO channel closed: {}", ctx.getChannel());
    }

    /**
     * Handle received messages
     */
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        Object msg = e.getMessage();
        if (msg instanceof TSOProto.Request) {
            TSOProto.Request request = (TSOProto.Request) msg;
            if (request.hasHandshakeRequest()) {
                checkHandshake(ctx, request.getHandshakeRequest());
                return;
            }
            if (!handshakeCompleted(ctx)) {
                LOG.error("Handshake not completed. Closing channel {}", ctx.getChannel());
                ctx.getChannel().close();
            }

            if (request.hasTimestampRequest()) {
                requestProcessor.timestampRequest(ctx.getChannel(), new MonitoringContext(metrics));
            } else if (request.hasCommitRequest()) {
                TSOProto.CommitRequest cr = request.getCommitRequest();
                requestProcessor.commitRequest(cr.getStartTimestamp(),
                                               cr.getCellIdList(),
                                               cr.getTableIdList(),
                                               cr.getIsRetry(),
                                               ctx.getChannel(),
                                               new MonitoringContext(metrics));
            } else if (request.hasFenceRequest()) {
                TSOProto.FenceRequest fr = request.getFenceRequest();
                requestProcessor.fenceRequest(fr.getTableId(), ctx.getChannel(), new MonitoringContext(metrics));
            } else {
                LOG.error("Invalid request {}. Closing channel {}", request, ctx.getChannel());
                ctx.getChannel().close();
            }
        } else {
            LOG.error("Unknown message type", msg);
        }
    }

    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        if (e.getCause() instanceof ClosedChannelException) {
            LOG.warn("ClosedChannelException caught. Cause: ", e.getCause());
            return;
        }
        LOG.warn("Unexpected exception from downstream. Closing channel {}", ctx.getChannel(), e.getCause());
        ctx.getChannel().close();
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Closeable implementation
    // ----------------------------------------------------------------------------------------------------------------
    @Override
    public void close() throws IOException {
        closeConnection();
        factory.releaseExternalResources();
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Helper methods and classes
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * Contains the required context for handshake
     */
    private static class TSOChannelContext {

        boolean handshakeComplete;

        TSOChannelContext() {
            handshakeComplete = false;
        }

        boolean getHandshakeComplete() {
            return handshakeComplete;
        }

        void setHandshakeComplete() {
            handshakeComplete = true;
        }

    }

    private void checkHandshake(final ChannelHandlerContext ctx, TSOProto.HandshakeRequest request) {

        TSOProto.HandshakeResponse.Builder response = TSOProto.HandshakeResponse.newBuilder();
        if (request.hasClientCapabilities()) {

            response.setClientCompatible(true)
                    .setServerCapabilities(TSOProto.Capabilities.newBuilder().build());
            TSOChannelContext tsoCtx = new TSOChannelContext();
            tsoCtx.setHandshakeComplete();
            ctx.setAttachment(tsoCtx);
        } else {
            response.setClientCompatible(false);
        }
        ctx.getChannel().write(TSOProto.Response.newBuilder().setHandshakeResponse(response.build()).build());

    }

    private boolean handshakeCompleted(ChannelHandlerContext ctx) {

        Object o = ctx.getAttachment();
        if (o instanceof TSOChannelContext) {
            TSOChannelContext tsoCtx = (TSOChannelContext) o;
            return tsoCtx.getHandshakeComplete();
        }
        return false;

    }

    /**
     * Netty pipeline configuration
     */
    static class TSOPipelineFactory implements ChannelPipelineFactory {

        private final ChannelHandler handler;

        TSOPipelineFactory(ChannelHandler handler) {
            this.handler = handler;
        }

        public ChannelPipeline getPipeline() throws Exception {

            ChannelPipeline pipeline = Channels.pipeline();
            // Max packet length is 10MB. Transactions with so many cells
            // that the packet is rejected will receive a ServiceUnavailableException.
            // 10MB is enough for 2 million cells in a transaction though.
            pipeline.addLast("lengthbaseddecoder", new LengthFieldBasedFrameDecoder(10 * 1024 * 1024, 0, 4, 0, 4));
            pipeline.addLast("lengthprepender", new LengthFieldPrepender(4));
            pipeline.addLast("protobufdecoder", new ProtobufDecoder(TSOProto.Request.getDefaultInstance()));
            pipeline.addLast("protobufencoder", new ProtobufEncoder());
            pipeline.addLast("handler", handler);

            return pipeline;
        }
    }

}
