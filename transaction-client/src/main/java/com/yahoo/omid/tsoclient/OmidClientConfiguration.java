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
package com.yahoo.omid.tsoclient;

import com.yahoo.omid.metrics.MetricsRegistry;
import com.yahoo.omid.metrics.NullMetricsProvider;

/**
 * Configuration for Omid client side
 */
public class OmidClientConfiguration {

    public static final String DEFAULT_TSO_HOST = "localhost";
    public static final int DEFAULT_TSO_PORT = 54758;
    public static final String DEFAULT_ZK_CLUSTER = "localhost:2181";
    public static final String DEFAULT_TSO_HOST_PORT_CONNECTION_STRING = DEFAULT_TSO_HOST + ":" + DEFAULT_TSO_PORT;
    public static final int DEFAULT_ZK_CONNECTION_TIMEOUT_IN_SECS = 10;
    public static final int DEFAULT_TSO_MAX_REQUEST_RETRIES = 5;
    public static final int DEFAULT_REQUEST_TIMEOUT_MS = 5000; // 5 secs
    public static final int DEFAULT_TSO_RECONNECTION_DELAY_SECS = 10;
    public static final int DEFAULT_TSO_RETRY_DELAY_MS = 1000;
    public static final int DEFAULT_TSO_EXECUTOR_THREAD_NUM = 3;

    public enum ConnType {
        DIRECT, ZK
    }

    private ConnType connectionType = ConnType.DIRECT;
    private String connectionString = DEFAULT_TSO_HOST_PORT_CONNECTION_STRING;
    private int zkConnectionTimeoutSecs = DEFAULT_ZK_CONNECTION_TIMEOUT_IN_SECS;
    private int requestMaxRetries = DEFAULT_TSO_MAX_REQUEST_RETRIES;
    private int requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MS;
    private int reconnectionDelaySecs = DEFAULT_TSO_RECONNECTION_DELAY_SECS;
    private int retryDelayMs = DEFAULT_TSO_RETRY_DELAY_MS;
    private int executorThreads =  DEFAULT_TSO_EXECUTOR_THREAD_NUM;
    private MetricsRegistry metrics = new NullMetricsProvider();


    // ----------------------------------------------------------------------------------------------------------------
    // Instantiation
    // ----------------------------------------------------------------------------------------------------------------

    public static OmidClientConfiguration create() {
        // Do additional stuff if required
        return new OmidClientConfiguration();
    }

    // Private constructor to avoid instantiation
    private OmidClientConfiguration() {
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Getters and setters for config params
    // ----------------------------------------------------------------------------------------------------------------


    public ConnType getConnectionType() {
        return connectionType;
    }

    public void setConnectionType(ConnType connectionType) {
        this.connectionType = connectionType;
    }

    public String getConnectionString() {
        return connectionString;
    }

    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    public int getZkConnectionTimeoutSecs() {
        return zkConnectionTimeoutSecs;
    }

    public void setZkConnectionTimeoutSecs(int zkConnectionTimeoutSecs) {
        this.zkConnectionTimeoutSecs = zkConnectionTimeoutSecs;
    }

    public int getRequestMaxRetries() {
        return requestMaxRetries;
    }

    public void setRequestMaxRetries(int requestMaxRetries) {
        this.requestMaxRetries = requestMaxRetries;
    }

    public int getRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    public void setRequestTimeoutMs(int requestTimeoutMs) {
        this.requestTimeoutMs = requestTimeoutMs;
    }

    public int getReconnectionDelaySecs() {
        return reconnectionDelaySecs;
    }

    public void setReconnectionDelaySecs(int reconnectionDelaySecs) {
        this.reconnectionDelaySecs = reconnectionDelaySecs;
    }

    public int getRetryDelayMs() {
        return retryDelayMs;
    }

    public void setRetryDelayMs(int retryDelayMs) {
        this.retryDelayMs = retryDelayMs;
    }

    public int getExecutorThreads() {
        return executorThreads;
    }

    public void setExecutorThreads(int executorThreads) {
        this.executorThreads = executorThreads;
    }

    public MetricsRegistry getMetrics() {
        return metrics;
    }

    public void setMetrics(MetricsRegistry metrics) {
        this.metrics = metrics;
    }

}
