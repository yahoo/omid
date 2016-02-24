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
import com.yahoo.omid.tsoclient.TSOClientConfiguration;
import com.yahoo.omid.tsoclient.TSOClientConfiguration.ConnType;

/**
 * Configuration for HBase's Omid client side
 */
public class HBaseOmidClientConfiguration {

    private final String commitTableName;
    private final TSOClientConfiguration TSOClientConfiguration;

    // ----------------------------------------------------------------------------------------------------------------
    // Builder definition and creation
    // ----------------------------------------------------------------------------------------------------------------

    public static class Builder {

        // Optional parameters for HBase - initialized to default values
        public String commitTableName = CommitTableConstants.COMMIT_TABLE_DEFAULT_NAME;
        public TSOClientConfiguration TSOClientConfiguration;

        // Delegate for TSOClientConfiguration
        private TSOClientConfiguration.Builder omidClientConfigBuilder = TSOClientConfiguration.builder();

        public Builder commitTableName(String val) {
            commitTableName = val;
            return this;
        }

        public Builder connectionType(ConnType val) {
            omidClientConfigBuilder.connectionType(val);
            return this;
        }

        public Builder connectionString(String val) {
            omidClientConfigBuilder.connectionString(val);
            return this;
        }

        public Builder zkConnectionTimeoutSecs(int val) {
            omidClientConfigBuilder.zkConnectionTimeoutSecs(val);
            return this;
        }

        public Builder requestMaxRetries(int val) {
            omidClientConfigBuilder.requestMaxRetries(val);
            return this;
        }

        public Builder requestTimeoutMs(int val) {
            omidClientConfigBuilder.requestTimeoutMs(val);
            return this;
        }

        public Builder reconnectionDelaySecs(int val) {
            omidClientConfigBuilder.reconnectionDelaySecs(val);
            return this;
        }

        public Builder retryDelayMs(int val) {
            omidClientConfigBuilder.retryDelayMs(val);
            return this;
        }

        public Builder executorThreads(int val) {
            omidClientConfigBuilder.executorThreads(val);
            return this;
        }

        public HBaseOmidClientConfiguration build() {
            // Build the delegate configuration...
            TSOClientConfiguration = omidClientConfigBuilder.build();
            // ... and finally the HBase config object
            return new HBaseOmidClientConfiguration(this);
        }
    }

    public static Builder builder() {
        // TODO: load config from file
        return new Builder();
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Private constructor to avoid instantiation
    // ----------------------------------------------------------------------------------------------------------------

    private HBaseOmidClientConfiguration(Builder builder) {
        commitTableName = builder.commitTableName;
        TSOClientConfiguration = builder.TSOClientConfiguration;
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Getters for config params
    // ----------------------------------------------------------------------------------------------------------------

    public String getCommitTableName() {
        return commitTableName;
    }

    public TSOClientConfiguration getTSOClientConfiguration() {
        return TSOClientConfiguration;
    }

}
