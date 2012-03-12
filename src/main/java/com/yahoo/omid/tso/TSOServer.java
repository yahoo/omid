/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.yahoo.omid.tso;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;

import com.yahoo.omid.tso.serialization.TSODecoder;
import com.yahoo.omid.tso.serialization.TSOEncoder;
import com.yahoo.omid.tso.persistence.BookKeeperStateBuilder;
import com.yahoo.omid.tso.persistence.StateBuilder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * TSO Server with serialization
 */
public class TSOServer implements Runnable {
    
    private static final Log LOG = LogFactory.getLog(BookKeeperStateBuilder.class);

    private TSOState state;
    private TSOServerConfig config;
    private boolean finish;
    private Object lock;

    public TSOServer() {
        super();
        this.config = TSOServerConfig.configFactory();
        
        this.finish = false;
        this.lock = new Object();
    }
    
    public TSOServer(TSOServerConfig config) {
        super();
        this.config = config;
        
        this.finish = false;
        this.lock = new Object();
    }
    
    public TSOState getState() {
        return state;
    }

    /**
     * Take two arguments :<br>
     * -port to listen to<br>
     * -nb of connections before shutting down
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // Print usage if no argument is specified.
        if (args.length < 1) {
            System.err.println("Usage: " + TSOServer.class.getSimpleName() + " <port>");
            return;
        }

        // Parse options.
        int port = Integer.parseInt(args[0]);
        int batch = Integer.parseInt(args[1]);
        int ensSize = Integer.parseInt(args[2]), qSize = Integer.parseInt(args[3]);
        String zkservers = args[4];

        new TSOServer(TSOServerConfig.configFactory(port, batch, (zkservers != null), ensSize, qSize, zkservers)).run();
    }

    @Override
    public void run() {
        // *** Start the Netty configuration ***
        // Start server with Nb of active threads = 2*NB CPU + 1 as maximum.
        ChannelFactory factory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool(), (Runtime.getRuntime().availableProcessors() * 2 + 1) * 2);

        ServerBootstrap bootstrap = new ServerBootstrap(factory);
        // Create the global ChannelGroup
        ChannelGroup channelGroup = new DefaultChannelGroup(TSOServer.class.getName());
        // threads max
        // int maxThreads = Runtime.getRuntime().availableProcessors() *2 + 1;
        int maxThreads = 5;
        // Memory limitation: 1MB by channel, 1GB global, 100 ms of timeout
        ThreadPoolExecutor pipelineExecutor = new OrderedMemoryAwareThreadPoolExecutor(maxThreads, 1048576, 1073741824,
                100, TimeUnit.MILLISECONDS, Executors.defaultThreadFactory());

        // This is the only object of timestamp oracle
        // TODO: make it singleton
        //TimestampOracle timestampOracle = new TimestampOracle();
        // The wrapper for the shared state of TSO
        state = BookKeeperStateBuilder.getState();
        
        if(state == null){
            LOG.error("Couldn't build state");
            return;
        }
        TSOState.BATCH_SIZE = config.getBatchSize();
        System.out.println("PARAM MAX_ITEMS: " + TSOState.MAX_ITEMS);
        System.out.println("PARAM BATCH_SIZE: " + TSOState.BATCH_SIZE);
        System.out.println("PARAM LOAD_FACTOR: " + TSOState.LOAD_FACTOR);
        System.out.println("PARAM MAX_THREADS: " + maxThreads);

        final TSOHandler handler = new TSOHandler(channelGroup, state);

        bootstrap.setPipelineFactory(new TSOPipelineFactory(pipelineExecutor, handler));
        bootstrap.setOption("tcpNoDelay", false);
        bootstrap.setOption("child.tcpNoDelay", false);
        bootstrap.setOption("child.keepAlive", true);
        bootstrap.setOption("child.reuseAddress", true);
        bootstrap.setOption("child.connectTimeoutMillis", 60000);
        bootstrap.setOption("readWriteFair", true);

        // *** Start the Netty running ***

        // Create the monitor
        ThroughputMonitor monitor = new ThroughputMonitor(state);
        // Add the parent channel to the group
        Channel channel = bootstrap.bind(new InetSocketAddress(config.getPort())));
        channelGroup.add(channel);
        
        // Compacter handler
        ChannelFactory comFactory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
              Executors.newCachedThreadPool(), (Runtime.getRuntime().availableProcessors() * 2 + 1) * 2);
        ServerBootstrap comBootstrap = new ServerBootstrap(comFactory);
        ChannelGroup comGroup = new DefaultChannelGroup("compacter");
        final CompacterHandler comHandler = new CompacterHandler(comGroup, state);
        comBootstrap.setPipelineFactory(new ChannelPipelineFactory() {

           @Override
           public ChannelPipeline getPipeline() throws Exception {
              ChannelPipeline pipeline = Channels.pipeline();
              pipeline.addLast("decoder", new ObjectDecoder());
              pipeline.addLast("encoder", new ObjectEncoder());
              pipeline.addLast("handler", comHandler);
              return pipeline;
           }
        });        
        comBootstrap.setOption("tcpNoDelay", false);
        comBootstrap.setOption("child.tcpNoDelay", false);
        comBootstrap.setOption("child.keepAlive", true);
        comBootstrap.setOption("child.reuseAddress", true);
        comBootstrap.setOption("child.connectTimeoutMillis", 100);
        comBootstrap.setOption("readWriteFair", true);
        channel = comBootstrap.bind(new InetSocketAddress(config.getPort() + 1));

        // Starts the monitor
        monitor.start();
        synchronized (lock) {
            while (!finish) {
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    break;
                }
            }
        }

        //timestampOracle.stop();
        handler.stop();
        comHandler.stop();
        state.stop();

        // *** Start the Netty shutdown ***

        // End the monitor
        System.out.println("End of monitor");
        monitor.interrupt();
        // Now close all channels
        System.out.println("End of channel group");
        channelGroup.close().awaitUninterruptibly();
        comGroup.close().awaitUninterruptibly();
        // Close the executor for Pipeline
        System.out.println("End of pipeline executor");
        pipelineExecutor.shutdownNow();
        // Now release resources
        System.out.println("End of resources");
        factory.releaseExternalResources();
        comFactory.releaseExternalResources();
    }

    public void stop() {
        finish = true;
    }
}
