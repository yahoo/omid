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
package com.yahoo.omid.transaction;

import static com.yahoo.omid.committable.hbase.HBaseCommitTable.HBASE_COMMIT_TABLE_NAME_KEY;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.CompactorScanner;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.hbase.HBaseCommitTable;
import com.yahoo.omid.committable.hbase.HBaseCommitTableConfig;

/**
 * Garbage collector for stale data: triggered upon HBase
 * compactions, it removes data from uncommitted transactions
 * older than the low watermark using a special scanner
 */
public class OmidCompactor extends BaseRegionObserver {

    private static final Logger LOG = LoggerFactory.getLogger(OmidCompactor.class);
    final static String OMID_COMPACTABLE_CF_FLAG = "OMID_ENABLED";

    private HBaseCommitTableConfig commitTableConf = null;
    private Configuration conf = null;
    @VisibleForTesting
    protected Queue<CommitTable.Client> commitTableClientQueue = new ConcurrentLinkedQueue<>();

    public OmidCompactor() {
        LOG.info("Compactor coprocessor initialized via empty constructor");
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        LOG.info("Starting compactor coprocessor");
        this.conf = env.getConfiguration();
        this.commitTableConf = new HBaseCommitTableConfig();
        String commitTableName = this.conf.get(HBASE_COMMIT_TABLE_NAME_KEY);
        if (commitTableName != null) {
            this.commitTableConf.setTableName(commitTableName);
        }
        LOG.info("Compactor coprocessor started");
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        LOG.info("Stopping compactor coprocessor");
        if (commitTableClientQueue != null) {
            for (CommitTable.Client commitTableClient : commitTableClientQueue) {
                commitTableClient.close();
            }
        }
        LOG.info("Compactor coprocessor stopped");
    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e,
                                      Store store,
                                      InternalScanner scanner,
                                      ScanType scanType,
                                      CompactionRequest request) throws IOException
    {
        HTableDescriptor desc = e.getEnvironment().getRegion().getTableDesc();
        HColumnDescriptor famDesc
            = desc.getFamily(Bytes.toBytes(store.getColumnFamilyName()));
        boolean omidCompactable = Boolean.valueOf(famDesc.getValue(OMID_COMPACTABLE_CF_FLAG));
        // only column families tagged as compactable are compacted
        // with omid compactor
        if (!omidCompactable) {
            return scanner;
        } else {
            CommitTable.Client commitTableClient = commitTableClientQueue.poll();
            if (commitTableClient == null) {
                commitTableClient = initAndGetCommitTableClient();
            }
            boolean isMajorCompaction = request.isMajor();
            return new CompactorScanner(e, scanner, commitTableClient, commitTableClientQueue, isMajorCompaction);
        }
    }

    private CommitTable.Client initAndGetCommitTableClient() throws IOException {
        LOG.info("Trying to get the commit table client");
        CommitTable commitTable = new HBaseCommitTable(conf, commitTableConf);
        try {
            CommitTable.Client commitTableClient = commitTable.getClient().get();
            LOG.info("Commit table client obtained {}", commitTableClient.getClass().getCanonicalName());
            return commitTableClient;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted getting the commit table client");
        } catch (ExecutionException ee) {
            throw new IOException("Error getting the commit table client", ee.getCause());
        }
    }

}
