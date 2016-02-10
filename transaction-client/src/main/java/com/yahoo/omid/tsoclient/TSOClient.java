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
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import java.io.IOException;
import java.util.Set;

/**
 * Describes the abstract methods to communicate to the TSO server
 */
public abstract class TSOClient {

    // ZK-related constants
    public static final String TSO_ZK_CLUSTER_CONFKEY = "tso.zkcluster";
    public static final String DEFAULT_ZK_CLUSTER = "localhost:2181";

    // Basic configuration constants & defaults
    public static final String ZK_CONNECTION_TIMEOUT_IN_SECS_CONFKEY = "tso.zk.connection.timeout-secs";
    public static final int DEFAULT_ZK_CONNECTION_TIMEOUT_IN_SECS = 10;
    public static final String TSO_HOST_CONFKEY = "tso.host";
    public static final String DEFAULT_TSO_HOST = "localhost";
    public static final String TSO_PORT_CONFKEY = "tso.port";
    public static final int DEFAULT_TSO_PORT = 54758;
    public static final String REQUEST_MAX_RETRIES_CONFKEY = "tso.request.max-retries";
    public static final int DEFAULT_TSO_MAX_REQUEST_RETRIES = 5;
    public static final String REQUEST_TIMEOUT_IN_MS_CONFKEY = "tso.request.timeout-ms";
    public static final int DEFAULT_REQUEST_TIMEOUT_MS = 5000; // 5 secs
    public static final String TSO_RECONNECTION_DELAY_SECS = "tso.reconnection_delay_secs";
    public static final int DEFAULT_TSO_RECONNECTION_DELAY_SECS = 10;
    public static final String TSO_RETRY_DELAY_MS_CONFKEY = "tso.retry_delay_ms";
    public static final int DEFAULT_TSO_RETRY_DELAY_MS = 1000;
    public static final String TSO_EXECUTOR_THREAD_NUM_CONFKEY = "tso.executor.threads";
    public static final int DEFAULT_TSO_EXECUTOR_THREAD_NUM = 3;

    private static final long DEFAULT_EPOCH = -1L;
    private volatile long epoch = DEFAULT_EPOCH;

    // ************* Abstract interface to communicate to the TSO *************

    public abstract TSOFuture<Long> getNewStartTimestamp();

    /**
     * @throws AbortException
     *             if the TSO server has aborted the transaction we try to commit.
     *
     * @throws ServiceUnavailableException
     *             if the request to the TSO server has been retried a certain 
     *             number of times and couldn't reach the endpoint
     */
    public abstract TSOFuture<Long> commit(long transactionId, Set<? extends CellId> cells);

    /**
     * @throws ClosingException
     *             if there's a problem when closing the link with the TSO server
     */
    public abstract TSOFuture<Void> close();

    // ****************** High availability related interface *****************

    /**
     * Sets the new epoch obtained from the new TSO
     * Used for high availability support.
     *
     * @param epoch the new epoch to set
     */
    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    /**
     * Returns the epoch of the TSO server that initialized this transaction.
     * Used for high availability support.
     */
    public long getEpoch() {
        return epoch;
    }

    // ************************* Useful exceptions ****************************

    /**
     * Thrown when a problem is found when creating the TSO client
     */
    public static class InstantiationException extends Exception {

        private static final long serialVersionUID = 8262507672984590285L;

        public InstantiationException(String message) {
            super(message);
        }

    }

    /**
     * Thrown when there are problems with the comm channel with the TSO server
     * (e.g. when it is closed or disconnected)
     */
    public static class ConnectionException extends IOException {

        private static final long serialVersionUID = -8489539195700267443L;

    }

    /**
     * Thrown when an error is produced when performing the actions required
     * to close the communication with the TSO server
     */
    public static class ClosingException extends Exception {

        private static final long serialVersionUID = -5681694952053689884L;

    }

    /**
     * Thrown when some incompatibilities between the TSO client & server are
     * found
     */
    public static class HandshakeFailedException extends Exception {

        private static final long serialVersionUID = 8545505066920548834L;

    }

    /**
     * Thrown when the requests from TSO client to the TSO server have reached
     * a number of retries
     */
    public static class ServiceUnavailableException extends Exception {

        private static final long serialVersionUID = -1551974284011474385L;

        public ServiceUnavailableException(String message) {
            super(message);
        }
    }

    /**
     * Thrown when the TSO server has aborted a transaction
     */
    public static class AbortException extends Exception {

        private static final long serialVersionUID = 1861474360100681040L;

    }

    /**
     * Thrown when a new TSO has been detected
     */
    public static class NewTSOException extends Exception {

        private static final long serialVersionUID = -3250655858200759321L;

    }

    // ****************************** Builder *********************************

    public static class Builder {

        private Configuration conf = new BaseConfiguration();
        private MetricsRegistry metrics = new NullMetricsProvider();

        public Builder withConfiguration(Configuration conf) {
            this.conf = conf;
            return this;
        }

        public Builder withMetrics(MetricsRegistry metrics) {
            this.metrics = metrics;
            return this;
        }

        public TSOClient build() {

            return new TSOClientImpl(conf, metrics);

        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

}
