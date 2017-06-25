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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.omid.metrics.NullMetricsProvider;
import org.apache.omid.proto.TSOProto;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyCollectionOf;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@SuppressWarnings({"UnusedDeclaration", "StatementWithEmptyBody"})
public class TestTSOChannelHandlerNetty {

    private static final Logger LOG = LoggerFactory.getLogger(TestTSOChannelHandlerNetty.class);

    @Mock
    private
    RequestProcessor requestProcessor;

    // Component under test
    private TSOChannelHandler channelHandler;

    @BeforeMethod
    public void beforeTestMethod() {
        MockitoAnnotations.initMocks(this);
        TSOServerConfig config = new TSOServerConfig();
        config.setPort(1434);
        channelHandler = new TSOChannelHandler(config, requestProcessor, new NullMetricsProvider());
    }

    @AfterMethod
    public void afterTestMethod() throws IOException {
        channelHandler.close();
    }

    @Test(timeOut = 10_000)
    public void testMainAPI() throws Exception {

        // Check initial state
        assertNull(channelHandler.listeningChannel);
        assertNull(channelHandler.channelGroup);

        // Check initial connection
        channelHandler.reconnect();
        assertTrue(channelHandler.listeningChannel.isOpen());
        assertEquals(channelHandler.channelGroup.size(), 1);
        assertEquals(((InetSocketAddress) channelHandler.listeningChannel.getLocalAddress()).getPort(), 1434);

        // Check connection close
        channelHandler.closeConnection();
        assertFalse(channelHandler.listeningChannel.isOpen());
        assertEquals(channelHandler.channelGroup.size(), 0);

        // Check re-closing connection
        channelHandler.closeConnection();
        assertFalse(channelHandler.listeningChannel.isOpen());
        assertEquals(channelHandler.channelGroup.size(), 0);

        // Check connection after closing
        channelHandler.reconnect();
        assertTrue(channelHandler.listeningChannel.isOpen());
        assertEquals(channelHandler.channelGroup.size(), 1);

        // Check re-connection
        channelHandler.reconnect();
        assertTrue(channelHandler.listeningChannel.isOpen());
        assertEquals(channelHandler.channelGroup.size(), 1);

        // Exercise closeable with re-connection trial
        channelHandler.close();
        assertFalse(channelHandler.listeningChannel.isOpen());
        assertEquals(channelHandler.channelGroup.size(), 0);
        try {
            channelHandler.reconnect();
        } catch (ChannelException e) {
            // Expected: Can't reconnect after closing
            assertFalse(channelHandler.listeningChannel.isOpen());
            assertEquals(channelHandler.channelGroup.size(), 0);
        }

    }

    @Test(timeOut = 10_000)
    public void testNettyConnectionToTSOFromClient() throws Exception {

        ClientBootstrap nettyClient = createNettyClientBootstrap();

        ChannelFuture channelF = nettyClient.connect(new InetSocketAddress("localhost", 1434));

        // ------------------------------------------------------------------------------------------------------------
        // Test the client can't connect cause the server is not there
        // ------------------------------------------------------------------------------------------------------------
        while (!channelF.isDone()) /** do nothing */ ;
        assertFalse(channelF.isSuccess());

        // ------------------------------------------------------------------------------------------------------------
        // Test creation of a server connection
        // ------------------------------------------------------------------------------------------------------------
        channelHandler.reconnect();
        assertTrue(channelHandler.listeningChannel.isOpen());
        // Eventually the channel group of the server should contain the listening channel
        assertEquals(channelHandler.channelGroup.size(), 1);

        // ------------------------------------------------------------------------------------------------------------
        // Test that a client can connect now
        // ------------------------------------------------------------------------------------------------------------
        channelF = nettyClient.connect(new InetSocketAddress("localhost", 1434));
        while (!channelF.isDone()) /** do nothing */ ;
        assertTrue(channelF.isSuccess());
        assertTrue(channelF.getChannel().isConnected());
        // Eventually the channel group of the server should have 2 elements
        while (channelHandler.channelGroup.size() != 2) /** do nothing */ ;

        // ------------------------------------------------------------------------------------------------------------
        // Close the channel on the client side and test we have one element less in the channel group
        // ------------------------------------------------------------------------------------------------------------
        channelF.getChannel().close().await();
        // Eventually the channel group of the server should have only one element
        while (channelHandler.channelGroup.size() != 1) /** do nothing */ ;

        // ------------------------------------------------------------------------------------------------------------
        // Open a new channel and test the connection closing on the server side through the channel handler
        // ------------------------------------------------------------------------------------------------------------
        channelF = nettyClient.connect(new InetSocketAddress("localhost", 1434));
        while (!channelF.isDone()) /** do nothing */ ;
        assertTrue(channelF.isSuccess());
        // Eventually the channel group of the server should have 2 elements again
        while (channelHandler.channelGroup.size() != 2) /** do nothing */ ;
        channelHandler.closeConnection();
        assertFalse(channelHandler.listeningChannel.isOpen());
        assertEquals(channelHandler.channelGroup.size(), 0);
        // Wait some time and check the channel was closed
        TimeUnit.SECONDS.sleep(1);
        assertFalse(channelF.getChannel().isOpen());

        // ------------------------------------------------------------------------------------------------------------
        // Test server re-connections with connected clients
        // ------------------------------------------------------------------------------------------------------------
        // Connect first time
        channelHandler.reconnect();
        assertTrue(channelHandler.listeningChannel.isOpen());
        // Eventually the channel group of the server should contain the listening channel
        assertEquals(channelHandler.channelGroup.size(), 1);
        // Check the client can connect
        channelF = nettyClient.connect(new InetSocketAddress("localhost", 1434));
        while (!channelF.isDone()) /** do nothing */ ;
        assertTrue(channelF.isSuccess());
        // Eventually the channel group of the server should have 2 elements
        while (channelHandler.channelGroup.size() != 2) /** do nothing */ ;
        // Re-connect and check that client connection was gone
        channelHandler.reconnect();
        assertTrue(channelHandler.listeningChannel.isOpen());
        // Eventually the channel group of the server should contain the listening channel
        assertEquals(channelHandler.channelGroup.size(), 1);
        // Wait some time and check the channel was closed
        TimeUnit.SECONDS.sleep(1);
        assertFalse(channelF.getChannel().isOpen());

        // ------------------------------------------------------------------------------------------------------------
        // Test closeable interface with re-connection trial
        // ------------------------------------------------------------------------------------------------------------
        channelHandler.close();
        assertFalse(channelHandler.listeningChannel.isOpen());
        assertEquals(channelHandler.channelGroup.size(), 0);
    }

    @Test(timeOut = 10_000)
    public void testNettyChannelWriting() throws Exception {

        // ------------------------------------------------------------------------------------------------------------
        // Prepare test
        // ------------------------------------------------------------------------------------------------------------

        // Connect channel handler
        channelHandler.reconnect();
        // Create client and connect it
        ClientBootstrap nettyClient = createNettyClientBootstrap();
        ChannelFuture channelF = nettyClient.connect(new InetSocketAddress("localhost", 1434));
        // Basic checks for connection
        while (!channelF.isDone()) /** do nothing */ ;
        assertTrue(channelF.isSuccess());
        assertTrue(channelF.getChannel().isConnected());
        Channel channel = channelF.getChannel();
        // Eventually the channel group of the server should have 2 elements
        while (channelHandler.channelGroup.size() != 2) /** do nothing */ ;
        // Write first handshake request
        TSOProto.HandshakeRequest.Builder handshake = TSOProto.HandshakeRequest.newBuilder();
        // NOTE: Add here the required handshake capabilities when necessary
        handshake.setClientCapabilities(TSOProto.Capabilities.newBuilder().build());
        channelF.getChannel().write(TSOProto.Request.newBuilder().setHandshakeRequest(handshake.build()).build());

        // ------------------------------------------------------------------------------------------------------------
        // Test channel writing
        // ------------------------------------------------------------------------------------------------------------
        testWritingTimestampRequest(channel);

        testWritingCommitRequest(channel);

        testWritingFenceRequest(channel);
    }

    private void testWritingTimestampRequest(Channel channel) throws InterruptedException {
        // Reset mock
        reset(requestProcessor);
        TSOProto.Request.Builder tsBuilder = TSOProto.Request.newBuilder();
        TSOProto.TimestampRequest.Builder tsRequestBuilder = TSOProto.TimestampRequest.newBuilder();
        tsBuilder.setTimestampRequest(tsRequestBuilder.build());
        // Write into the channel
        channel.write(tsBuilder.build()).await();
        verify(requestProcessor, timeout(100).times(1)).timestampRequest(any(Channel.class), any(MonitoringContext.class));
        verify(requestProcessor, timeout(100).never())
                .commitRequest(anyLong(), anyCollectionOf(Long.class), anyCollectionOf(Long.class), anyBoolean(), any(Channel.class), any(MonitoringContext.class));
    }

    private void testWritingCommitRequest(Channel channel) throws InterruptedException {
        // Reset mock
        reset(requestProcessor);
        TSOProto.Request.Builder commitBuilder = TSOProto.Request.newBuilder();
        TSOProto.CommitRequest.Builder commitRequestBuilder = TSOProto.CommitRequest.newBuilder();
        commitRequestBuilder.setStartTimestamp(666);
        commitRequestBuilder.addCellId(666);
        commitBuilder.setCommitRequest(commitRequestBuilder.build());
        TSOProto.Request r = commitBuilder.build();
        assertTrue(r.hasCommitRequest());
        // Write into the channel
        channel.write(commitBuilder.build()).await();
        verify(requestProcessor, timeout(100).never()).timestampRequest(any(Channel.class), any(MonitoringContext.class));
        verify(requestProcessor, timeout(100).times(1))
                .commitRequest(eq(666L), anyCollectionOf(Long.class), anyCollectionOf(Long.class), eq(false), any(Channel.class), any(MonitoringContext.class));
    }

    private void testWritingFenceRequest(Channel channel) throws InterruptedException {
        // Reset mock
        reset(requestProcessor);
        TSOProto.Request.Builder fenceBuilder = TSOProto.Request.newBuilder();
        TSOProto.FenceRequest.Builder fenceRequestBuilder = TSOProto.FenceRequest.newBuilder();
        fenceRequestBuilder.setTableId(666);
        fenceBuilder.setFenceRequest(fenceRequestBuilder.build());
        TSOProto.Request r = fenceBuilder.build();
        assertTrue(r.hasFenceRequest());
        // Write into the channel
        channel.write(fenceBuilder.build()).await();
        verify(requestProcessor, timeout(100).never()).timestampRequest(any(Channel.class), any(MonitoringContext.class));
        verify(requestProcessor, timeout(100).times(1))
                .fenceRequest(eq(666L), any(Channel.class), any(MonitoringContext.class));
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Helper methods
    // ----------------------------------------------------------------------------------------------------------------

    private ClientBootstrap createNettyClientBootstrap() {

        ChannelFactory factory = new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(
                        new ThreadFactoryBuilder().setNameFormat("client-boss-%d").build()),
                Executors.newCachedThreadPool(
                        new ThreadFactoryBuilder().setNameFormat("client-worker-%d").build()), 1);
        // Create the bootstrap
        ClientBootstrap bootstrap = new ClientBootstrap(factory);
        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("keepAlive", true);
        bootstrap.setOption("reuseAddress", true);
        bootstrap.setOption("connectTimeoutMillis", 100);
        ChannelPipeline pipeline = bootstrap.getPipeline();
        pipeline.addLast("lengthbaseddecoder", new LengthFieldBasedFrameDecoder(8 * 1024, 0, 4, 0, 4));
        pipeline.addLast("lengthprepender", new LengthFieldPrepender(4));
        pipeline.addLast("protobufdecoder", new ProtobufDecoder(TSOProto.Response.getDefaultInstance()));
        pipeline.addLast("protobufencoder", new ProtobufEncoder());
        pipeline.addLast("handler", new SimpleChannelHandler() {

            @Override
            public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
                LOG.info("Channel {} connected", ctx.getChannel());
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
                LOG.error("Error on channel {}", ctx.getChannel(), e.getCause());
            }

        });
        return bootstrap;
    }

}
