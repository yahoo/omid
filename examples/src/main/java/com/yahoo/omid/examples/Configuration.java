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
package com.yahoo.omid.examples;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Objects;
import com.yahoo.omid.committable.hbase.CommitTableConstants;
import com.yahoo.omid.tools.hbase.HBaseLogin;
import com.yahoo.omid.transaction.HBaseOmidClientConfiguration;
import com.yahoo.omid.tsoclient.TSOClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Configuration extends HBaseLogin.Config {
    private static final Logger LOG = LoggerFactory.getLogger(Configuration.class);

    @Parameter(names = "-help", description = "Print command options and exit", help = true)
    boolean help = false;
    @Parameter(names = "-hbaseConfig", description = "Path to hbase-site.xml. Loads from classpath if not specified")
    String hbaseConfig = "N/A";
    @Parameter(names = "-hadoopConfig", description = "Path to core-site.xml. Loads from classpath if not specified")
    String hadoopConfig = "N/A";
    @Parameter(names = "-tsoHostPort", description = "TSO host & port. Format: <host>:<port>")
    String tsoHostPort = TSOClient.DEFAULT_TSO_HOST + ":" + TSOClient.DEFAULT_TSO_PORT;
    @Parameter(names = "-commitTableName", description = "CommitTable name")
    String commitTableName = CommitTableConstants.COMMIT_TABLE_DEFAULT_NAME;
    @Parameter(names = "-userTableName", description = "User table name")
    String userTableName = "MY_TX_TABLE";
    @Parameter(names = "-cf", description = "User table column family")
    String cfName = "MY_CF";

    // ----------------------------------------------------------------------------------------------------------------
    // Configuration creation
    // ----------------------------------------------------------------------------------------------------------------

    // Avoid instantiation
    private Configuration() {
    }

    static Configuration parse(String[] commandLineArgs) {
        Configuration commandLineConfig = new Configuration();

        JCommander commandLine = new JCommander(commandLineConfig);
        try {
            commandLine.setProgramName(getCallerClass(2).getCanonicalName());
            commandLine.parse(commandLineArgs);
        } catch (ParameterException ex) {
            commandLine.usage();
            throw new IllegalArgumentException(ex.getMessage());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        if (commandLineConfig.hasHelpFlag()) {
            commandLine.usage();
            System.exit(0);
        }
        LOG.info("{}", commandLineConfig);
        return commandLineConfig;
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Helper methods
    // ----------------------------------------------------------------------------------------------------------------

    private static Class getCallerClass(int level) throws ClassNotFoundException {
        StackTraceElement[] stElements = Thread.currentThread().getStackTrace();
        String rawFQN = stElements[level + 1].toString().split("\\(")[0];
        return Class.forName(rawFQN.substring(0, rawFQN.lastIndexOf('.')));
    }

    HBaseOmidClientConfiguration createOmidClientConfig() {
        HBaseOmidClientConfiguration hbaseOmidClientConfig = HBaseOmidClientConfiguration.create();
        hbaseOmidClientConfig.setConnectionString(tsoHostPort);
        return hbaseOmidClientConfig;
    }

    private boolean hasHelpFlag() {
        return help;
    }

    public String toString() {
        return Objects.toStringHelper(this)
                .add("TSO host:port", tsoHostPort)
                .add("HBase conf path", hbaseConfig)
                .add("Hadoop conf path", hadoopConfig)
                .add("Commit Table", commitTableName)
                .add("User table", userTableName)
                .add("ColFam", cfName)
                .toString();
    }

}
