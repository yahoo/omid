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
import com.yahoo.omid.tsoclient.OmidClientConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * Configuration for HBase's Omid client side
 */
public class HBaseOmidClientConfiguration {

    private Configuration hbaseConfiguration = HBaseConfiguration.create();
    private String commitTableName = CommitTableConstants.COMMIT_TABLE_DEFAULT_NAME;

    // Delegated class
    private OmidClientConfiguration omidClientConfiguration = OmidClientConfiguration.create();

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

    public OmidClientConfiguration getOmidClientConfiguration() {
        return omidClientConfiguration;
    }

    public void setOmidClientConfiguration(OmidClientConfiguration omidClientConfiguration) {
        this.omidClientConfiguration = omidClientConfiguration;
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Getters and setters of the delegated class
    // ----------------------------------------------------------------------------------------------------------------

    public void setMetrics(MetricsRegistry metrics) {
        omidClientConfiguration.setMetrics(metrics);
    }

    public OmidClientConfiguration.ConnType getConnectionType() {
        return omidClientConfiguration.getConnectionType();
    }

    public void setConnectionString(String connectionString) {
        omidClientConfiguration.setConnectionString(connectionString);
    }

    public void setExecutorThreads(int executorThreads) {
        omidClientConfiguration.setExecutorThreads(executorThreads);
    }

    public int getReconnectionDelaySecs() {
        return omidClientConfiguration.getReconnectionDelaySecs();
    }

    public int getRequestTimeoutMs() {
        return omidClientConfiguration.getRequestTimeoutMs();
    }

    public void setReconnectionDelaySecs(int reconnectionDelaySecs) {
        omidClientConfiguration.setReconnectionDelaySecs(reconnectionDelaySecs);
    }

    public void setConnectionType(OmidClientConfiguration.ConnType connectionType) {
        omidClientConfiguration.setConnectionType(connectionType);
    }

    public MetricsRegistry getMetrics() {
        return omidClientConfiguration.getMetrics();
    }

    public void setRequestTimeoutMs(int requestTimeoutMs) {
        omidClientConfiguration.setRequestTimeoutMs(requestTimeoutMs);
    }

    public String getConnectionString() {
        return omidClientConfiguration.getConnectionString();
    }

    public void setZkConnectionTimeoutSecs(int zkConnectionTimeoutSecs) {
        omidClientConfiguration.setZkConnectionTimeoutSecs(zkConnectionTimeoutSecs);
    }

    public int getRetryDelayMs() {
        return omidClientConfiguration.getRetryDelayMs();
    }

    public int getExecutorThreads() {
        return omidClientConfiguration.getExecutorThreads();
    }

    public void setRequestMaxRetries(int requestMaxRetries) {
        omidClientConfiguration.setRequestMaxRetries(requestMaxRetries);
    }

    public void setRetryDelayMs(int retryDelayMs) {
        omidClientConfiguration.setRetryDelayMs(retryDelayMs);
    }

    public int getZkConnectionTimeoutSecs() {
        return omidClientConfiguration.getZkConnectionTimeoutSecs();
    }

    public int getRequestMaxRetries() {
        return omidClientConfiguration.getRequestMaxRetries();
    }

}
