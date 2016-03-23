/**
 * Copyright 2011-2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.omid.tso;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Module;
import com.yahoo.omid.YAMLUtils;
import com.yahoo.omid.metrics.MetricsRegistry;
import com.yahoo.omid.tools.hbase.SecureHBaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

/**
 * Reads the configuration parameters of a TSO server instance from CONFIG_FILE_NAME.
 * If file CONFIG_FILE_NAME is missing defaults to DEFAULT_CONFIG_FILE_NAME
 */
@SuppressWarnings("all")
public class TSOServerConfig extends SecureHBaseConfig {

    private static final Logger LOG = LoggerFactory.getLogger(TSOServerConfig.class);

    private static final String DEFAULT_CONFIG_FILE_NAME = "default-omid.yml";
    private static final String CONFIG_FILE_NAME = "omid.yml";

    private static final String LINUX_TSO_NET_IFACE_PREFIX = "eth";
    private static final String MAC_TSO_NET_IFACE_PREFIX = "en";

    // ----------------------------------------------------------------------------------------------------------------
    // Instantiation
    // ----------------------------------------------------------------------------------------------------------------
    public TSOServerConfig() {
        this(CONFIG_FILE_NAME);
    }

    @VisibleForTesting
    TSOServerConfig(String configFileName) {
        new YAMLUtils().loadSettings(configFileName, DEFAULT_CONFIG_FILE_NAME, this);
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Configuration parameters
    // ----------------------------------------------------------------------------------------------------------------

    private Module timestampStoreModule;

    private Module commitTableStoreModule;

    private Module leaseModule;

    private int port;

    private MetricsRegistry metrics;

    private int maxItems;

    private int maxBatchSize;

    private int batchPersistTimeoutMS;

    private int persistHandlerNum;

    private int numBuffersPerHandler;

    private String networkIfaceName = getDefaultNetworkInterface();

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }

    public void setBatchPersistTimeoutMS(int value) {
        this.batchPersistTimeoutMS = value;
    }

    public String getNetworkIfaceName() {
        return networkIfaceName;
    }

    public void setNetworkIfaceName(String networkIfaceName) {
        this.networkIfaceName = networkIfaceName;
    }

    public Module getTimestampStoreModule() {
        return timestampStoreModule;
    }

    public void setTimestampStoreModule(Module timestampStoreModule) {
        this.timestampStoreModule = timestampStoreModule;
    }

    public Module getCommitTableStoreModule() {
        return commitTableStoreModule;
    }

    public void setCommitTableStoreModule(Module commitTableStoreModule) {
        this.commitTableStoreModule = commitTableStoreModule;
    }

    public Module getLeaseModule() {
        return leaseModule;
    }

    public void setLeaseModule(Module leaseModule) {
        this.leaseModule = leaseModule;
    }

    public int getMaxItems() {
        return maxItems;
    }

    public void setMaxItems(int maxItems) {
        this.maxItems = maxItems;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public int getBatchPersistTimeoutMS() {
        return batchPersistTimeoutMS;
    }

    public MetricsRegistry getMetrics() {
        return metrics;
    }

    public void setMetrics(MetricsRegistry metrics) {
        this.metrics = metrics;
    }

    public int getPersistHandlerNum() {
        return persistHandlerNum;
    }

    public void setPersistHandlerNum(int persistHandlerNum) {
        this.persistHandlerNum = persistHandlerNum;
    }

    public int getNumBuffersPerHandler() {
        return numBuffersPerHandler;
    }

    public void setNumBuffersPerHandler(int numBuffersPerHandler) {
        this.numBuffersPerHandler = numBuffersPerHandler;
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Helper methods
    // ----------------------------------------------------------------------------------------------------------------

    private String getDefaultNetworkInterface() {

        try {
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements()) {
                String name = networkInterfaces.nextElement().getDisplayName();
                LOG.info("Iterating over network interfaces, found '{}'", name);
                if (name.startsWith(MAC_TSO_NET_IFACE_PREFIX) || name.startsWith(LINUX_TSO_NET_IFACE_PREFIX)) {
                    return name;
                }
            }
        } catch (SocketException ignored) {
            throw new RuntimeException("Failed to find any network interfaces", ignored);
        }
        throw new IllegalArgumentException(String.format("No network '%s*'/'%s*' interfaces found",
                                                         MAC_TSO_NET_IFACE_PREFIX, LINUX_TSO_NET_IFACE_PREFIX));

    }

}
