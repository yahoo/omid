/**
 * Copyright 2011-2015 Yahoo Inc.
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
package com.yahoo.omid.tso.hbase;

import static com.yahoo.omid.tso.hbase.HBaseTimestampStorage.TIMESTAMP_TABLE_DEFAULT_NAME;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.yahoo.omid.HBaseShims;
import com.yahoo.omid.committable.hbase.HBaseLogin;

public class CreateTable {

    private static final Logger LOG = LoggerFactory.getLogger(CreateTable.class);

    static class Config {

        @Parameter(names = "-tableName", description = "Name of the timestamp storage table in HBase", required = false)
        String table = TIMESTAMP_TABLE_DEFAULT_NAME;

        @ParametersDelegate
        HBaseLogin.Config loginFlags = new HBaseLogin.Config();
    }

    public static void main(String[] args) throws IOException {

        Config config = new Config();
        new JCommander(config, args);

        HBaseLogin.loginIfNeeded(config.loginFlags);
        createTable(HBaseConfiguration.create(), config.table);
    }

    public static void createTable(Configuration hbaseConf, String tableName) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(hbaseConf);

        if (!admin.tableExists(tableName)) {
            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
            HColumnDescriptor datafam = new HColumnDescriptor(HBaseTimestampStorage.TSO_FAMILY);
            datafam.setMaxVersions(3);
            HBaseShims.addFamilyToHTableDescriptor(desc, datafam);
            admin.createTable(desc);
        }

        if (admin.isTableDisabled(tableName)) {
            admin.enableTable(tableName);
        }
        admin.close();
        LOG.info("Table {} created successfully", tableName);
    }

}
