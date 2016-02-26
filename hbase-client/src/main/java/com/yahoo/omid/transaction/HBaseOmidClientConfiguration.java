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
package com.yahoo.omid.transaction;

import com.yahoo.omid.committable.hbase.CommitTableConstants;
import com.yahoo.omid.metrics.MetricsRegistry;
import com.yahoo.omid.tsoclient.TSOClientConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * Configuration for HBase's Omid client side
 */
public class HBaseOmidClientConfiguration {

    private Configuration hbaseConfiguration = HBaseConfiguration.create();
    private String commitTableName = CommitTableConstants.COMMIT_TABLE_DEFAULT_NAME;

    // Delegated class
    private TSOClientConfiguration tsoClientConfiguration = TSOClientConfiguration.create();

    // ----------------------------------------------------------------------------------------------------------------
    // Instantiation
    // ----------------------------------------------------------------------------------------------------------------

    public static HBaseOmidClientConfiguration create() {
        // TODO Add additional stuff if required (e.g. read from config file)
        return new HBaseOmidClientConfiguration();
    }

    // Private constructor to avoid instantiation
    private HBaseOmidClientConfiguration() {
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Getters and setters for config params
    // ----------------------------------------------------------------------------------------------------------------

    public Configuration getHBaseConfiguration() {
        return hbaseConfiguration;
    }

    public void setHBaseConfiguration(Configuration hbaseConfiguration) {
        this.hbaseConfiguration = hbaseConfiguration;
    }

    public String getCommitTableName() {
        return commitTableName;
    }

    public void setCommitTableName(String commitTableName) {
        this.commitTableName = commitTableName;
    }

    public TSOClientConfiguration getTSOClientConfiguration() {
        return tsoClientConfiguration;
    }

    public void setTSOClientConfiguration(TSOClientConfiguration tsoClientConfiguration) {
        this.tsoClientConfiguration = tsoClientConfiguration;
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Getters and setters of the delegated class
    // ----------------------------------------------------------------------------------------------------------------

    public void setMetrics(MetricsRegistry metrics) {
        tsoClientConfiguration.setMetrics(metrics);
    }

    public TSOClientConfiguration.ConnType getConnectionType() {
        return tsoClientConfiguration.getConnectionType();
    }

    public void setConnectionString(String connectionString) {
        tsoClientConfiguration.setConnectionString(connectionString);
    }

    public void setExecutorThreads(int executorThreads) {
        tsoClientConfiguration.setExecutorThreads(executorThreads);
    }

    public int getReconnectionDelaySecs() {
        return tsoClientConfiguration.getReconnectionDelaySecs();
    }

    public int getRequestTimeoutMs() {
        return tsoClientConfiguration.getRequestTimeoutMs();
    }

    public void setReconnectionDelaySecs(int reconnectionDelaySecs) {
        tsoClientConfiguration.setReconnectionDelaySecs(reconnectionDelaySecs);
    }

    public void setConnectionType(TSOClientConfiguration.ConnType connectionType) {
        tsoClientConfiguration.setConnectionType(connectionType);
    }

    public MetricsRegistry getMetrics() {
        return tsoClientConfiguration.getMetrics();
    }

    public void setRequestTimeoutMs(int requestTimeoutMs) {
        tsoClientConfiguration.setRequestTimeoutMs(requestTimeoutMs);
    }

    public String getConnectionString() {
        return tsoClientConfiguration.getConnectionString();
    }

    public void setZkConnectionTimeoutSecs(int zkConnectionTimeoutSecs) {
        tsoClientConfiguration.setZkConnectionTimeoutSecs(zkConnectionTimeoutSecs);
    }

    public int getRetryDelayMs() {
        return tsoClientConfiguration.getRetryDelayMs();
    }

    public int getExecutorThreads() {
        return tsoClientConfiguration.getExecutorThreads();
    }

    public void setRequestMaxRetries(int requestMaxRetries) {
        tsoClientConfiguration.setRequestMaxRetries(requestMaxRetries);
    }

    public void setRetryDelayMs(int retryDelayMs) {
        tsoClientConfiguration.setRetryDelayMs(retryDelayMs);
    }

    public int getZkConnectionTimeoutSecs() {
        return tsoClientConfiguration.getZkConnectionTimeoutSecs();
    }

    public int getRequestMaxRetries() {
        return tsoClientConfiguration.getRequestMaxRetries();
    }

}
