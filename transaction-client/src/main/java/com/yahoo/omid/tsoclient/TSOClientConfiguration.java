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

/**
 * Configuration for Omid client side
 */
public class TSOClientConfiguration {

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

    private final ConnType connType;
    private final String connString;
    private final int requestMaxRetries;
    private final int zkConnectionTimeoutSecs;
    private final int requestTimeoutMs;
    private final int reconnectionDelaySecs;
    private final int retryDelayMs;
    private final int executorThreads;

    // ----------------------------------------------------------------------------------------------------------------
    // Builder definition and creation
    // ----------------------------------------------------------------------------------------------------------------

    public static class Builder {
        // Required parameters

        // Optional parameters - initialized to default values
        private ConnType connType = ConnType.DIRECT;
        private String connString = DEFAULT_TSO_HOST_PORT_CONNECTION_STRING;
        private int zkConnectionTimeoutSecs = DEFAULT_ZK_CONNECTION_TIMEOUT_IN_SECS;
        private int requestMaxRetries = DEFAULT_TSO_MAX_REQUEST_RETRIES;
        private int requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MS;
        private int reconnectionDelaySecs = DEFAULT_TSO_RECONNECTION_DELAY_SECS;
        private int retryDelayMs = DEFAULT_TSO_RETRY_DELAY_MS;
        private int executorThreads =  DEFAULT_TSO_EXECUTOR_THREAD_NUM;

        public Builder connectionType(ConnType val) {
            this.connType = val;
            return this;
        }

        public Builder connectionString(String val) {
            this.connString = val;
            return this;
        }

        public Builder zkConnectionTimeoutSecs(int val) {
            zkConnectionTimeoutSecs = val;
            return this;
        }

        public Builder requestMaxRetries(int val) {
            requestMaxRetries = val;
            return this;
        }

        public Builder requestTimeoutMs(int val) {
            requestTimeoutMs = val;
            return this;
        }

        public Builder reconnectionDelaySecs(int val) {
            reconnectionDelaySecs = val;
            return this;
        }

        public Builder retryDelayMs(int val) {
            retryDelayMs = val;
            return this;
        }

        public Builder executorThreads(int val) {
            executorThreads = val;
            return this;
        }

        public TSOClientConfiguration build() {
            return new TSOClientConfiguration(this);
        }

    }

    public static Builder builder() {
        return new Builder();
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Private constructor to avoid instantiation
    // ----------------------------------------------------------------------------------------------------------------

    private TSOClientConfiguration(Builder builder) {
        connType = builder.connType;
        connString = builder.connString;
        requestMaxRetries = builder.requestMaxRetries;
        zkConnectionTimeoutSecs = builder.zkConnectionTimeoutSecs;
        requestTimeoutMs = builder.requestTimeoutMs;
        reconnectionDelaySecs = builder.reconnectionDelaySecs;
        retryDelayMs = builder.retryDelayMs;
        executorThreads = builder.executorThreads;
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Getters for config params
    // ----------------------------------------------------------------------------------------------------------------

    public ConnType getConnType() {
        return connType;
    }

    public String getConnString() {
        return connString;
    }

    public int getRequestMaxRetries() {
        return requestMaxRetries;
    }

    public int getZkConnectionTimeoutSecs() {
        return zkConnectionTimeoutSecs;
    }

    public int getRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    public int getReconnectionDelaySecs() {
        return reconnectionDelaySecs;
    }

    public int getRetryDelayMs() {
        return retryDelayMs;
    }

    public int getExecutorThreads() {
        return executorThreads;
    }
}
