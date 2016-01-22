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
package com.yahoo.omid.committable.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.yahoo.omid.HBaseShims;
import com.yahoo.omid.committable.hbase.HBaseCommitTable.KeyGenerator;

public class CreateTable {

    private static final Logger LOG = LoggerFactory.getLogger(CreateTable.class);

    static class Config {

        @Parameter(names = "-tableName", description = "Name of the commit table in HBase", required = false)
        String table = HBaseCommitTable.COMMIT_TABLE_DEFAULT_NAME;

        @Parameter(names = "-numSplits", description = "Number of splits (to pre-split table)", required = false)
        int numSplits = 1;

        @ParametersDelegate
        HBaseLogin.Config loginFlags = new HBaseLogin.Config();
    }

    public static void main(String[] args) throws IOException {
        Config config = new Config();
        new JCommander(config, args);
        HBaseLogin.loginIfNeeded(config.loginFlags);
        createTable(HBaseConfiguration.create(), config.table, config.numSplits);
    }

    public static void createTable(Configuration hbaseConf, String tableName, int numSplits) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(hbaseConf);

        if (!admin.tableExists(tableName)) {
            KeyGenerator keyGen = HBaseCommitTable.defaultKeyGenerator();

            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
            HColumnDescriptor datafam = new HColumnDescriptor(HBaseCommitTable.COMMIT_TABLE_FAMILY);
            datafam.setMaxVersions(1);
            HBaseShims.addFamilyToHTableDescriptor(desc, datafam);
            HColumnDescriptor lowWatermarkFam = new HColumnDescriptor(
                    HBaseCommitTable.LOW_WATERMARK_FAMILY);
            lowWatermarkFam.setMaxVersions(1);
            HBaseShims.addFamilyToHTableDescriptor(desc, lowWatermarkFam);
            if (numSplits > 1) {
                RegionSplitter.SplitAlgorithm algo = RegionSplitter.newSplitAlgoInstance(hbaseConf,
                        RegionSplitter.UniformSplit.class.getName());
                algo.setFirstRow(algo.rowToStr(keyGen.startTimestampToKey(0)));
                algo.setLastRow(algo.rowToStr(keyGen.startTimestampToKey(Long.MAX_VALUE)));
                admin.createTable(desc, algo.split(numSplits));
            } else {
                admin.createTable(desc);
            }

            LOG.info("Created {} table with {} regions",
                    tableName, admin.getTableRegions(Bytes.toBytes(tableName)).size());
        }

        if (admin.isTableDisabled(tableName)) {
            admin.enableTable(tableName);
        }
        admin.close();
        LOG.info("Table {} created successfully", tableName);
    }

}
