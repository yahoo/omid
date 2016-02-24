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

import com.beust.jcommander.IVariableArity;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.annotations.VisibleForTesting;
import com.yahoo.omid.metrics.MetricsProvider;
import com.yahoo.omid.tools.hbase.HBaseLogin;
import com.yahoo.omid.tsoclient.TSOClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;

import static com.yahoo.omid.committable.hbase.CommitTableConstants.COMMIT_TABLE_DEFAULT_NAME;
import static com.yahoo.omid.timestamp.storage.HBaseTimestampStorage.TIMESTAMP_TABLE_DEFAULT_NAME;
import static com.yahoo.omid.timestamp.storage.ZKTimestampStorage.DEFAULT_ZK_CLUSTER;
import static com.yahoo.omid.tso.PersistenceProcessorImpl.DEFAULT_BATCH_PERSIST_TIMEOUT_MS;
import static com.yahoo.omid.tso.PersistenceProcessorImpl.DEFAULT_PERSIST_HANDLER_NUM;
import static com.yahoo.omid.tso.PersistenceProcessorImpl.DEFAULT_MAX_BATCH_SIZE;
import static com.yahoo.omid.tso.RequestProcessorImpl.DEFAULT_MAX_ITEMS;

/**
 * Holds the configuration parameters of a TSO server instance.
 */
public class TSOServerCommandLineConfig extends JCommander implements IVariableArity {

    private static final Logger LOG = LoggerFactory.getLogger(TSOServer.class);

    private static final String LINUX_TSO_NET_IFACE_PREFIX = "eth";
    private static final String MAC_TSO_NET_IFACE_PREFIX = "en";
    private static final String NETWORK_IFACE_ENV_VAR = "networkIface";

    enum TimestampStore {
        MEMORY("com.yahoo.omid.tso.InMemoryTimestampStorageModule"),
        HBASE("com.yahoo.omid.timestamp.storage.HBaseTimestampStorageModule"),
        ZK("com.yahoo.omid.timestamp.storage.ZKTimestampStorageModule"),;

        private String clazz;

        TimestampStore(String className) {
            clazz = className;
        }

        @Override
        public String toString() {
            return clazz;
        }
    }

    enum CommitTableStore {
        MEMORY("com.yahoo.omid.tso.InMemoryCommitTableStorageModule"),
        HBASE("com.yahoo.omid.committable.hbase.HBaseCommitTableStorageModule");

        private String clazz;

        CommitTableStore(String className) {
            clazz = className;
        }

        @Override
        public String toString() {
            return clazz;
        }
    }

    // ------------------------------------------------------------------------
    // Configuration creation
    // ------------------------------------------------------------------------

    // Avoid instantiation
    private TSOServerCommandLineConfig(String[] args) {
        super();
        addObject(this);
        parse(args);
        setProgramName(TSOServer.class.getName());
    }

    // Main method
    public static TSOServerCommandLineConfig parseConfig(String args[]) {
        return new TSOServerCommandLineConfig(args);
    }

    @VisibleForTesting
    static TSOServerCommandLineConfig defaultConfig() {
        return parseConfig(new String[]{});
    }

    // ------------------------------------------------------------------------
    // Configuration parameters
    // ------------------------------------------------------------------------

    @Parameter(names = "-help", description = "Print command options and exit", help = true)
    private boolean help = false;

    @Parameter(names = "-timestampStore", description = "Available stores, MEMORY, HBASE, ZK")
    private TimestampStore timestampStore = TimestampStore.MEMORY;

    @Parameter(names = "-zkCluster", description = "Zookeeper cluster in form: <host>:<port>,<host>,<port>,...)")
    private String zkCluster = DEFAULT_ZK_CLUSTER;

    @Parameter(names = "-commitTableStore", description = "Available stores, MEMORY, HBASE")
    private CommitTableStore commitTableStore = CommitTableStore.MEMORY;

    @Parameter(names = "-hbaseTimestampTable", description = "HBase timestamp table name")
    private String hbaseTimestampTable = TIMESTAMP_TABLE_DEFAULT_NAME;

    @Parameter(names = "-hbaseCommitTable", description = "HBase commit table name")
    private String hbaseCommitTable = COMMIT_TABLE_DEFAULT_NAME;

    @Parameter(names = "-port", description = "Port reserved by the Status Oracle")
    private int port = TSOClient.DEFAULT_TSO_PORT;

    @Parameter(names = "-metricsProviderModule", description = "Guice metrics provider config, e.g. com.yahoo.omid.metrics.CodahaleModule")
    private String metricsProviderModule = "com.yahoo.omid.metrics.CodahaleModule";

    @Parameter(names = "-metricsConfigs", description = "Metrics config", variableArity = true)
    private List<String> metricsConfigs = new ArrayList<>(Arrays.asList(MetricsProvider.CODAHALE_METRICS_CONFIG));

    @Parameter(names = "-maxItems", description = "Maximum number of items in the TSO (will determine the 'low watermark')")
    private int maxItems = DEFAULT_MAX_ITEMS;

    @Parameter(names = "-maxBatchSize", description = "Maximum size in each persisted batch of commits")
    private int maxBatchSize = DEFAULT_MAX_BATCH_SIZE;

    @Parameter(names = "-batchPersistTimeout", description = "Number of milliseconds the persist processer will wait without new input before flushing a batch")
    private int batchPersistTimeoutMS = DEFAULT_BATCH_PERSIST_TIMEOUT_MS;

    @Parameter(names = "-persistHandlerNum", description = "Number of handlers that writes to the commit table")
    private int persistHandlerNum = DEFAULT_PERSIST_HANDLER_NUM;
    
    // TODO This is probably going to be temporary. So, we should remove it later if not required. Otherwise
    // we should make it private and provide accessors as is done with the other parameters
    @Parameter(names = "-publishHostAndPortInZK", description = "Publishes the host:port of this TSO server in ZK")
    public boolean shouldHostAndPortBePublishedInZK = false;

    @Parameter(names = "-"
            + NETWORK_IFACE_ENV_VAR, description = "Network Interface where TSO is attached to (e.g. eth0, en0...)")
    private String networkIfaceName = getDefaultNetworkIntf();

    @Parameter(names = "-leasePeriodInMs", description = "Lease period for high availability in ms")
    private long leasePeriodInMs = TSOServer.DEFAULT_LEASE_PERIOD_IN_MSECS;

    @ParametersDelegate
    private HBaseLogin.Config loginFlags = new HBaseLogin.Config();

    @Override
    public int processVariableArity(String optionName,
                                    String[] options) {
        int i = 0;
        for (String o : options) {
            if (o.startsWith("-")) {
                return i;
            }
            i++;
        }
        return i;
    }

    public boolean hasHelpFlag() {
        return help;
    }

    public TimestampStore getTimestampStore() {

        return timestampStore;
    }

    public void setZKCluster(String zkCluster) {
        this.zkCluster = zkCluster;
    }

    public String getZKCluster() {
        return zkCluster;
    }

    public CommitTableStore getCommitTableStore() {
        return commitTableStore;
    }

    public String getTimestampTable() {
        return hbaseTimestampTable;
    }

    public String getCommitTable() {
        return hbaseCommitTable;
    }

    public int getPort() {
        return port;
    }

    public String getMetricsProviderModule() {
        return metricsProviderModule;
    }

    public List<String> getMetricsConfigs() {
        return metricsConfigs;
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

    public int getPersistHandlerNum() {
        return persistHandlerNum;
    }

    public HBaseLogin.Config getLoginFlags() {
        return loginFlags;
    }

    public String getNetworkIface() {
        return networkIfaceName;
    }

    public void setLeasePeriodInMs(long leasePeriodInMs) {
        this.leasePeriodInMs = leasePeriodInMs;
    }

    public long getLeasePeriodInMs() {
        return leasePeriodInMs;
    }

    // ************************* Helper methods *******************************

    private String getDefaultNetworkIntf() {
        try {
            String envVar = System.getProperty(NETWORK_IFACE_ENV_VAR);
            if (envVar != null && envVar.length() > 0) {
                return envVar;
            }
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
