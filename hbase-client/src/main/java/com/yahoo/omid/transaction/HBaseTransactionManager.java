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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.CommitTable.CommitTimestamp;
import com.yahoo.omid.committable.hbase.HBaseCommitTable;
import com.yahoo.omid.committable.hbase.HBaseCommitTableConfig;
import com.yahoo.omid.metrics.MetricsRegistry;
import com.yahoo.omid.tsoclient.CellId;
import com.yahoo.omid.tsoclient.TSOClient;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class HBaseTransactionManager extends AbstractTransactionManager implements HBaseTransactionClient {

    static final byte[] SHADOW_CELL_SUFFIX = "\u0080".getBytes(Charsets.UTF_8); // Non printable char (128 ASCII)

    private static class HBaseTransactionFactory implements TransactionFactory<HBaseCellId> {

        @Override
        public HBaseTransaction createTransaction(long transactionId, long epoch, AbstractTransactionManager tm) {

            return new HBaseTransaction(transactionId, epoch, new HashSet<HBaseCellId>(), tm);

        }

    }

    // ----------------------------------------------------------------------------------------------------------------
    // Construction
    // ----------------------------------------------------------------------------------------------------------------

    public static TransactionManager newInstance() throws OmidInstantiationException {
        return newInstance(HBaseOmidClientConfiguration.create());
    }

    public static TransactionManager newInstance(HBaseOmidClientConfiguration hBaseOmidClientConfiguration)
            throws OmidInstantiationException
    {

        try {
            TSOClient tsoClient = TSOClient.newInstance(hBaseOmidClientConfiguration.getTSOClientConfiguration());
            HBaseCommitTableConfig commitTableConf =
                    new HBaseCommitTableConfig(hBaseOmidClientConfiguration.getCommitTableName());
            CommitTable commitTable =
                    new HBaseCommitTable(hBaseOmidClientConfiguration.getHBaseConfiguration(), commitTableConf);
            CommitTable.Client commitTableClient = commitTable.getClient().get();
            return new HBaseTransactionManager(hBaseOmidClientConfiguration.getMetrics(),
                                               tsoClient,
                                               commitTableClient,
                                               new HBaseTransactionFactory());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OmidInstantiationException("Interrupted whilst creating the HBase transaction manager", e);
        } catch (ExecutionException e) {
            throw new OmidInstantiationException("Exception whilst getting the CommitTable client", e);
        }

    }

    @VisibleForTesting
    static class Builder {
        // Required parameters
        private HBaseOmidClientConfiguration hbaseOmidClientConf;

        // Optional parameters - initialized to default values
        private TSOClient tsoClient;
        private CommitTable.Client commitTableClient;

        private Builder(HBaseOmidClientConfiguration hbaseOmidClientConf) {
            this.hbaseOmidClientConf = hbaseOmidClientConf;
        }

        Builder tsoClient(TSOClient tsoClient) {
            this.tsoClient = tsoClient;
            return this;
        }

        Builder commitTableClient(CommitTable.Client client) {
            this.commitTableClient = client;
            return this;
        }

        HBaseTransactionManager build() throws OmidInstantiationException {

            // TODO: Should improve this as it's very ugly. It's here from the very beginning and allows test
            // to configure specific components
            if (tsoClient == null) {
                tsoClient = TSOClient.newInstance(hbaseOmidClientConf.getTSOClientConfiguration());
            }
            // TODO: Should improve this as it's very ugly. It's here from the very beginning and allows test
            // to configure specific components
            if (commitTableClient == null) {
                try {
                    HBaseCommitTableConfig commitTableConf =
                            new HBaseCommitTableConfig(hbaseOmidClientConf.getCommitTableName());
                    CommitTable commitTable =
                            new HBaseCommitTable(hbaseOmidClientConf.getHBaseConfiguration(), commitTableConf);
                    commitTableClient = commitTable.getClient().get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new OmidInstantiationException("Interrupted whilst creating the HBase transaction manager", e);
                } catch (ExecutionException e) {
                    throw new OmidInstantiationException("Exception whilst getting the CommitTable client", e);
                }
            }
            return new HBaseTransactionManager(hbaseOmidClientConf.getMetrics(), tsoClient, commitTableClient, new HBaseTransactionFactory());
        }

    }

    @VisibleForTesting
    static Builder builder() {
        return builder(HBaseOmidClientConfiguration.create());
    }

    @VisibleForTesting
    static Builder builder(HBaseOmidClientConfiguration hbaseOmidClientConf) {
        return new Builder(hbaseOmidClientConf);
    }

    private HBaseTransactionManager(MetricsRegistry metrics,
                                    TSOClient tsoClient,
                                    CommitTable.Client commitTableClient,
                                    HBaseTransactionFactory hBaseTransactionFactory) {
        super(metrics, tsoClient, commitTableClient, hBaseTransactionFactory);
    }

    // ----------------------------------------------------------------------------------------------------------------
    // AbstractTransactionManager overwritten methods
    // ----------------------------------------------------------------------------------------------------------------

    @Override
    public void updateShadowCells(AbstractTransaction<? extends CellId> tx)
            throws TransactionManagerException {

        HBaseTransaction transaction = enforceHBaseTransactionAsParam(tx);

        Set<HBaseCellId> cells = transaction.getWriteSet();

        // Add shadow cells
        for (HBaseCellId cell : cells) {
            Put put = new Put(cell.getRow());
            put.add(cell.getFamily(),
                    CellUtils.addShadowCellSuffix(cell.getQualifier()),
                    transaction.getStartTimestamp(),
                    Bytes.toBytes(transaction.getCommitTimestamp()));
            try {
                cell.getTable().put(put);
            } catch (IOException e) {
                throw new TransactionManagerException(
                        "Failed inserting shadow cell " + cell + " for Tx " + transaction, e);
            }
        }
        // Flush affected tables before returning to avoid loss of shadow cells updates when autoflush is disabled
        try {
            transaction.flushTables();
        } catch (IOException e) {
            throw new TransactionManagerException("Exception while flushing writes", e);
        }
    }

    @Override
    public void preCommit(AbstractTransaction<? extends CellId> transaction) throws TransactionManagerException {
        try {
            // Flush all pending writes
            HBaseTransaction hBaseTx = enforceHBaseTransactionAsParam(transaction);
            hBaseTx.flushTables();
        } catch (IOException e) {
            throw new TransactionManagerException("Exception while flushing writes", e);
        }
    }

    @Override
    public void preRollback(AbstractTransaction<? extends CellId> transaction) throws TransactionManagerException {
        try {
            // Flush all pending writes
            HBaseTransaction hBaseTx = enforceHBaseTransactionAsParam(transaction);
            hBaseTx.flushTables();
        } catch (IOException e) {
            throw new TransactionManagerException("Exception while flushing writes", e);
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // HBaseTransactionClient method implementations
    // ----------------------------------------------------------------------------------------------------------------

    @Override
    public boolean isCommitted(HBaseCellId hBaseCellId) throws TransactionException {
        try {
            CommitTimestamp tentativeCommitTimestamp =
                    locateCellCommitTimestamp(hBaseCellId.getTimestamp(), tsoClient.getEpoch(),
                                              new CommitTimestampLocatorImpl(hBaseCellId, Maps.<Long, Long>newHashMap()));

            // If transaction that added the cell was invalidated
            if (!tentativeCommitTimestamp.isValid()) {
                return false;
            }

            switch (tentativeCommitTimestamp.getLocation()) {
                case COMMIT_TABLE:
                case SHADOW_CELL:
                    return true;
                case NOT_PRESENT:
                    return false;
                case CACHE: // cache was empty
                default:
                    return false;
            }
        } catch (IOException e) {
            throw new TransactionException("Failure while checking if a transaction was committed", e);
        }
    }

    @Override
    public long getLowWatermark() throws TransactionException {
        try {
            return commitTableClient.readLowWatermark().get();
        } catch (ExecutionException ee) {
            throw new TransactionException("Error reading low watermark", ee.getCause());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new TransactionException("Interrupted reading low watermark", ie);
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Helper methods
    // ----------------------------------------------------------------------------------------------------------------

    private HBaseTransaction
    enforceHBaseTransactionAsParam(AbstractTransaction<? extends CellId> tx) {

        if (tx instanceof HBaseTransaction) {
            return (HBaseTransaction) tx;
        } else {
            throw new IllegalArgumentException(
                    "The transaction object passed is not an instance of HBaseTransaction");
        }

    }

    static class CommitTimestampLocatorImpl implements CommitTimestampLocator {
        private HBaseCellId hBaseCellId;
        private final Map<Long, Long> commitCache;

        public CommitTimestampLocatorImpl(HBaseCellId hBaseCellId, Map<Long, Long> commitCache) {
            this.hBaseCellId = hBaseCellId;
            this.commitCache = commitCache;
        }

        @Override
        public Optional<Long> readCommitTimestampFromCache(long startTimestamp) {
            if (commitCache.containsKey(startTimestamp)) {
                return Optional.of(commitCache.get(startTimestamp));
            }
            return Optional.absent();
        }

        @Override
        public Optional<Long> readCommitTimestampFromShadowCell(long startTimestamp) throws IOException {

            Get get = new Get(hBaseCellId.getRow());
            byte[] family = hBaseCellId.getFamily();
            byte[] shadowCellQualifier = CellUtils.addShadowCellSuffix(hBaseCellId.getQualifier());
            get.addColumn(family, shadowCellQualifier);
            get.setMaxVersions(1);
            get.setTimeStamp(startTimestamp);
            Result result = hBaseCellId.getTable().get(get);
            if (result.containsColumn(family, shadowCellQualifier)) {
                return Optional.of(Bytes.toLong(result.getValue(family, shadowCellQualifier)));
            }
            return Optional.absent();
        }

    }

}
