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
import com.google.inject.Module;

import org.apache.omid.NetworkUtils;
import org.apache.omid.YAMLUtils;
import org.apache.omid.metrics.MetricsRegistry;
import org.apache.omid.tools.hbase.SecureHBaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads the configuration parameters of a TSO server instance from CONFIG_FILE_NAME.
 * If file CONFIG_FILE_NAME is missing defaults to DEFAULT_CONFIG_FILE_NAME
 */
@SuppressWarnings("all")
public class TSOServerConfig extends SecureHBaseConfig {

    private static final Logger LOG = LoggerFactory.getLogger(TSOServerConfig.class);

    private static final String CONFIG_FILE_NAME = "omid-server-configuration.yml";
    private static final String DEFAULT_CONFIG_FILE_NAME = "default-omid-server-configuration.yml";

    public static enum WAIT_STRATEGY {
        HIGH_THROUGHPUT,
        LOW_CPU
    };

    public static enum TIMESTAMP_TYPE {
      INCREMENTAL,
      WORLD_TIME
    };

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

    private int conflictMapSize;

    private int numConcurrentCTWriters;

    private int batchSizePerCTWriter;

    private int batchPersistTimeoutInMs;

    private String waitStrategy;

    private String networkIfaceName = NetworkUtils.getDefaultNetworkInterface();

    private String timestampType;

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getConflictMapSize() {
        return conflictMapSize;
    }

    public void setConflictMapSize(int conflictMapSize) {
        this.conflictMapSize = conflictMapSize;
    }

    public int getNumConcurrentCTWriters() {
        return numConcurrentCTWriters;
    }

    public void setNumConcurrentCTWriters(int numConcurrentCTWriters) {
        this.numConcurrentCTWriters = numConcurrentCTWriters;
    }

    public int getBatchSizePerCTWriter() {
        return batchSizePerCTWriter;
    }

    public void setBatchSizePerCTWriter(int batchSizePerCTWriter) {
        this.batchSizePerCTWriter = batchSizePerCTWriter;
    }

    public int getBatchPersistTimeoutInMs() {
        return batchPersistTimeoutInMs;
    }

    public void setBatchPersistTimeoutInMs(int value) {
        this.batchPersistTimeoutInMs = value;
    }

    public String getNetworkIfaceName() {
        return networkIfaceName;
    }

    public void setNetworkIfaceName(String networkIfaceName) {
        this.networkIfaceName = networkIfaceName;
    }

    public String getTimestampType() {
        return timestampType;
    }

    public void setTimestampType(String type) {
        this.timestampType = type;
    }

    public TIMESTAMP_TYPE getTimestampTypeEnum() {
        return TSOServerConfig.TIMESTAMP_TYPE.valueOf(timestampType);
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

    public MetricsRegistry getMetrics() {
        return metrics;
    }

    public void setMetrics(MetricsRegistry metrics) {
        this.metrics = metrics;
    }

    public String getWaitStrategy() {
        return waitStrategy;
    }

    public WAIT_STRATEGY getWaitStrategyEnum() {
        return TSOServerConfig.WAIT_STRATEGY.valueOf(waitStrategy);
    }

    public void setWaitStrategy(String waitStrategy) {
        this.waitStrategy = waitStrategy;
    }
}
