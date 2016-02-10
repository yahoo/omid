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

import com.google.common.base.Optional;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.CommitTable.CommitTimestamp;
import com.yahoo.omid.metrics.Counter;
import com.yahoo.omid.metrics.MetricsRegistry;
import com.yahoo.omid.metrics.Timer;
import com.yahoo.omid.transaction.Transaction.Status;
import com.yahoo.omid.tsoclient.CellId;
import com.yahoo.omid.tsoclient.TSOClient;
import com.yahoo.omid.tsoclient.TSOClient.AbortException;
import com.yahoo.omid.tsoclient.TSOClient.ConnectionException;
import com.yahoo.omid.tsoclient.TSOClient.NewTSOException;
import com.yahoo.omid.tsoclient.TSOClient.ServiceUnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static com.yahoo.omid.committable.CommitTable.CommitTimestamp.Location.*;
import static com.yahoo.omid.metrics.MetricsUtils.name;

/**
 * Omid's base abstract implementation of the
 * {@link TransactionManagerExtension} interface.
 *
 * Provides extra methods to allow transaction manager developers to perform
 * different actions before/after the methods exposed by the
 * {@link TransactionManager} interface.
 *
 * So, this abstract class must be extended by particular implementations of
 * transaction managers related to different storage systems (HBase...)
 */
public abstract class AbstractTransactionManager implements TransactionManager {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractTransactionManager.class);

    public interface TransactionFactory<T extends CellId> {

        public AbstractTransaction<T> createTransaction(long transactionId,
                                                        long epoch,
                                                        AbstractTransactionManager tm);

    }

    private final MetricsRegistry metrics;
    protected final TSOClient tsoClient;
    private final boolean ownsTSOClient;
    protected final CommitTable.Client commitTableClient;
    private final boolean ownsCommitTableClient;
    private final TransactionFactory<? extends CellId> transactionFactory;

    // Metrics
    private final Timer startTimestampTimer;
    private final Timer commitTimer;
    private final Timer commitTableUpdateTimer;
    private final Timer shadowCellsUpdateTimer;
    private final Counter committedTxsCounter;
    private final Counter rolledbackTxsCounter;
    private final Counter errorTxsCounter;
    private final Counter invalidatedTxsCounter;

    /**
     * Base constructor
     *
     * @param metrics
     *            allows to add metrics to this component
     * @param tsoClient
     *            a client for accessing functionality of the status oracle
     * @param ownsTSOClient
     *            whether this transaction manager owns or not the TSO client
     *            instance passed. This is used to close the resources properly.
     * @param commitTableClient
     *            a client for accessing functionality of the commit table
     * @param ownsCommitTableClient
     *            whether this transaction manager owns or not the commit table
     *            client instance passed. This is used to close the resources
     *            properly.
     * @param transactionFactory
     *            a transaction factory to create the specific transaction
     *            objects required by the transaction manager being implemented.
     */
    public AbstractTransactionManager(MetricsRegistry metrics,
                                      TSOClient tsoClient,
                                      boolean ownsTSOClient,
                                      CommitTable.Client commitTableClient,
                                      boolean ownsCommitTableClient,
                                      TransactionFactory<? extends CellId> transactionFactory) {
        this.metrics = metrics;
        this.tsoClient = tsoClient;
        this.ownsTSOClient = ownsTSOClient;
        this.commitTableClient = commitTableClient;
        this.ownsCommitTableClient = ownsCommitTableClient;
        this.transactionFactory = transactionFactory;

        // Metrics configuration
        this.startTimestampTimer = metrics.timer(name("omid", "tm", "hbase", "startTimestamp", "latency"));
        this.commitTimer = metrics.timer(name("omid", "tm", "hbase", "commit", "latency"));
        this.commitTableUpdateTimer = metrics.timer(name("omid", "tm", "hbase", "commitTableUpdate", "latency"));
        this.shadowCellsUpdateTimer = metrics.timer(name("omid", "tm", "hbase", "shadowCellsUpdate", "latency"));
        this.committedTxsCounter = metrics.counter(name("omid", "tm", "hbase", "committedTxs"));
        this.rolledbackTxsCounter = metrics.counter(name("omid", "tm", "hbase", "rolledbackTxs"));
        this.errorTxsCounter = metrics.counter(name("omid", "tm", "hbase", "erroredTxs"));
        this.invalidatedTxsCounter = metrics.counter(name("omid", "tm", "hbase", "invalidatedTxs"));
    }

    /**
     * Allows specific implementations to update the shadow cells.
     * @param transaction
     *            the transaction to update shadow cells for
     * @throws TransactionManagerException
     */
    public abstract void updateShadowCells(AbstractTransaction<? extends CellId> transaction)
            throws TransactionManagerException;

    /**
     * Allows transaction manager developers to perform actions before
     * creating a transaction.
     * @throws TransactionManagerException
     */
    public void preBegin() throws TransactionManagerException {};

    /**
     * @see com.yahoo.omid.transaction.TransactionManager#begin()
     */
    @Override
    public final Transaction begin() throws TransactionException {

        try {
            try {
                preBegin();
            } catch (TransactionManagerException e) {
                LOG.warn(e.getMessage());
            }

            long startTimestamp, epoch;

            // The loop is required for HA scenarios where we get the timestamp
            // but when getting the epoch, the client is connected to a new TSOServer
            // When this happen, the epoch will be larger than the startTimestamp,
            // so we need to start the transaction again. We use the fact that epoch
            // is always smaller or equal to a timestamp, and therefore, we first need
            // to get the timestamp and then the epoch.
            long startTimeInNs = System.nanoTime();
            do {
                startTimestamp = tsoClient.getNewStartTimestamp().get();
                epoch = tsoClient.getEpoch();
            } while (epoch > startTimestamp);
            startTimestampTimer.update(System.nanoTime() - startTimeInNs);

            AbstractTransaction<? extends CellId> tx =
                    transactionFactory.createTransaction(startTimestamp, epoch, this);
            try {
                postBegin(tx);
            } catch (TransactionManagerException e) {
                LOG.warn(e.getMessage());
            }
            return tx;
        } catch (ExecutionException e) {
            throw new TransactionException("Could not get new timestamp", e);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new TransactionException("Interrupted getting timestamp", ie);
        }
    }

    /**
     * Allows transaction manager developers to perform actions after
     * having started a transaction.
     * @param transaction
     *            the transaction that was just created.
     * @throws TransactionManagerException
     */
    public void postBegin(AbstractTransaction<? extends CellId> transaction)
            throws TransactionManagerException {};

    /**
     * Allows transaction manager developers to perform actions before
     * committing a transaction.
     * @param transaction
     *            the transaction that is going to be committed.
     * @throws TransactionManagerException
     */
    public void preCommit(AbstractTransaction<? extends CellId> transaction)
            throws TransactionManagerException {};

    /**
     * @see com.yahoo.omid.transaction.TransactionManager#commit()
     */
    @Override
    public final void commit(Transaction transaction)
            throws RollbackException, TransactionException {

        AbstractTransaction<? extends CellId> tx = enforceAbstractTransactionAsParam(transaction);
        enforceTransactionIsInRunningState(tx);

        if (tx.isRollbackOnly()) { // If the tx was marked to rollback, do it
            rollback(tx);
            throw new RollbackException("Transaction was set to rollback");
        }

        try {
            try {
                preCommit(tx);
            } catch (TransactionManagerException e) {
                tx.cleanup();
                throw new TransactionException(e.getMessage(), e);
            }
            long startTimeInNs = System.nanoTime();
            long commitTs = tsoClient.commit(tx.getStartTimestamp(), tx.getWriteSet()).get();
            commitTimer.update(System.nanoTime() - startTimeInNs);
            completeCommittedTx(tx, commitTs, true);
            committedTxsCounter.inc();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof AbortException) { // Conflicts detected, so rollback
                rollback(tx);
                rolledbackTxsCounter.inc();
                throw new RollbackException("Conflicts detected in tx writeset. Transaction aborted.", e.getCause());
            } else if (e.getCause() instanceof ServiceUnavailableException
                       ||
                       e.getCause() instanceof NewTSOException
                       ||
                       e.getCause() instanceof ConnectionException) {

                errorTxsCounter.inc();
                try {
                    LOG.warn("Can't contact the TSO for receiving outcome for Tx {}. Checking commit table...", tx);
                    // Check the commit table to try to find if the target
                    // TSO woke up in the meantime and put the commit
                    // TODO: Decide what we should we do if we can not contact the commit table
                    Optional<CommitTimestamp> commitTimestamp =
                            commitTableClient.getCommitTimestamp(tx.getStartTimestamp()).get();
                    if (commitTimestamp.isPresent()) {
                        if (commitTimestamp.get().isValid()) {
                            long commitTS = commitTimestamp.get().getValue();
                            completeCommittedTx(tx, commitTS, false);
                            LOG.warn("Valid commit TS found in Commit Table for Tx {} and was committed", tx);
                        } else {
                            rollback(tx);
                            LOG.warn("Tx {} was found invalidated in Commit Table and was rolled back", tx);
                            throw new RollbackException(tx + " rolled-back."
                                                        + " Invalidated by another TX started in a new TSO",
                                                        e.getCause());
                        }
                    } else {
                        LOG.warn("Trying to invalidate Tx {} proactively", tx);
                        boolean invalidated = commitTableClient.tryInvalidateTransaction(tx.getStartTimestamp()).get();
                        if (invalidated) {
                            invalidatedTxsCounter.inc();
                            rollback(tx);
                            LOG.warn("Tx {} invalidated and rolled back", tx);
                            throw new RollbackException(tx + " rolled-back preventively."
                                                        + " Other TSO was detected.",
                                                        e.getCause());
                        } else {
                            LOG.warn("Tx {} appeared committed in Commit Table and wasn't invalidated. "
                                    + "Double checking Commit Table...", tx);
                            // TODO: Decide what we should we do if we can not contact the commit table
                            commitTimestamp = commitTableClient.getCommitTimestamp(tx.getStartTimestamp()).get();
                            if (commitTimestamp.isPresent() && commitTimestamp.get().isValid()) {
                                completeCommittedTx(tx, commitTimestamp.get().getValue(), false);
                                LOG.warn("Valid commit TS found in Commit Table for Tx {} and was committedd", tx);
                            } else {
                                LOG.error("Can't determine the outcome of Tx {}", tx);
                                throw new TransactionException("Cannot determite the outcome of " + tx);
                            }
                        }

                    }
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                    throw new TransactionException("Interrupted reading commit TS from Commit Table", e1);
                } catch (ExecutionException e1) {
                    throw new TransactionException("Could not commit. Interrupted reading commit TS from Commit Table",
                            e1);
                }
            } else {
                throw new TransactionException("Could not commit", e.getCause());
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new TransactionException("Interrupted committing transaction", ie);
        }

    }

    /**
     * Allows transaction manager developers to perform actions after
     * commiting a transaction.
     * @param transaction
     *            the transaction that was committed.
     * @throws TransactionManagerException
     */
    public void postCommit(AbstractTransaction<? extends CellId> transaction)
            throws TransactionManagerException {};

    /**
     * Allows transaction manager developers to perform actions before
     * rolling-back a transaction.
     * @param transaction
     *            the transaction that is going to be rolled-back.
     * @throws TransactionManagerException
     */
    public void preRollback(AbstractTransaction<? extends CellId> transaction)
            throws TransactionManagerException {};

    /**
     * @see com.yahoo.omid.transaction.TransactionManager#rollback()
     */
    @Override
    public final void rollback(Transaction transaction)
            throws TransactionException {

        AbstractTransaction<? extends CellId> tx = enforceAbstractTransactionAsParam(transaction);
        enforceTransactionIsInRunningState(tx);

        try {
            try {
                preRollback(tx);
            } catch (TransactionManagerException e) {
                throw new TransactionException(e.getMessage(), e);
            }
            // Make sure its commit timestamp is 0, so the cleanup does the right job
            tx.setCommitTimestamp(0);
            tx.setStatus(Status.ROLLEDBACK);
            try {
                postRollback(tx);
            } catch (TransactionManagerException e) {
                LOG.warn(e.getMessage());
            }
        } finally {
            tx.cleanup();
        }

    }

    /**
     * Allows transaction manager developers to perform actions after
     * rolling-back a transaction.
     * @param transaction
     *            the transaction that was rolled-back.
     * @throws TransactionManagerException
     */
    public void postRollback(AbstractTransaction<? extends CellId> transaction)
            throws TransactionManagerException {};

    /**
     * Check if the transaction commit data is in the shadow cell
     * @param cellStartTimestamp
     *            the transaction start timestamp
     *        locator
     *            the timestamp locator
     * @throws InterruptedException, ExecutionException
     */
    Optional<CommitTimestamp> readCommitTimestampFromShadowCell(long cellStartTimestamp,
                                                                 CommitTimestampLocator locator)
            throws IOException {
        Optional<CommitTimestamp> commitTS = Optional.absent();

        Optional<Long> commitTimestamp =
                locator.readCommitTimestampFromShadowCell(cellStartTimestamp);
        if (commitTimestamp.isPresent()) {
            commitTS = Optional.of(// Valid commit Timestamp
                    new CommitTimestamp(SHADOW_CELL, commitTimestamp.get(), true));
        }

        return commitTS;
    }

    /**
     * This function returns the commit timestamp for a particular cell
     * if the transaction was already committed in the system. In case the
     * transaction was not committed and the cell was written by transaction
     * initialized by a previous TSO server, an invalidation try occurs.
     * Otherwise the function returns a value that indicates that the commit
     * timestamp was not found.
     * @param cellStartTimestamp
     *          start timestamp of the cell to locate the commit timestamp for.
     * @param epoch
     *          the epoch of the TSO server the current tso client is working with.
     * @param locator
     *          a locator to find the commit timestamp in the system.
     * @return the commit timestamp joint with the location where it was found
     *         or an object indicating that it was not found in the system
     * @throws IOException
     */
    public CommitTimestamp locateCellCommitTimestamp(long cellStartTimestamp, long epoch,
            CommitTimestampLocator locator) throws IOException {

        try {
            // 1) First check the cache
            Optional<Long> commitTimestamp =
                    locator.readCommitTimestampFromCache(cellStartTimestamp);
            if (commitTimestamp.isPresent()) { // Valid commit timestamp
                return new CommitTimestamp(CACHE, commitTimestamp.get(), true);
            }

            // 2) Then check the commit table
            // If the data was written at a previous epoch,
            // check whether the transaction was invalidated

            Optional<CommitTimestamp> commitTimeStamp =
                    commitTableClient.getCommitTimestamp(cellStartTimestamp).get();
            if (commitTimeStamp.isPresent()) {
                return commitTimeStamp.get();
            }

            // 3) Read from shadow cell

            commitTimeStamp = readCommitTimestampFromShadowCell(cellStartTimestamp, locator);
            if (commitTimeStamp.isPresent()) {
                return commitTimeStamp.get();
            }

            // 4) Check the epoch and invalidate the entry

            // if the data was written by a transaction from a previous epoch (previous TSO)
            if (cellStartTimestamp < epoch) {
                boolean invalidated = commitTableClient.tryInvalidateTransaction(cellStartTimestamp).get();
                if (invalidated) { // Invalid commit timestamp
                    return new CommitTimestamp(COMMIT_TABLE, CommitTable.INVALID_TRANSACTION_MARKER, false);
                }
            }

            // 5) We did not manage to invalidate the transactions then
            // check the commit table
            commitTimeStamp = commitTableClient.getCommitTimestamp(cellStartTimestamp).get();
            if (commitTimeStamp.isPresent()) {
                return commitTimeStamp.get();
            }

            // 6) Read from shadow cell
            commitTimeStamp = readCommitTimestampFromShadowCell(cellStartTimestamp, locator);
            if (commitTimeStamp.isPresent()) {
                return commitTimeStamp.get();
            }

            // *) Otherwise return not found
            return new CommitTimestamp(NOT_PRESENT, -1L /** TODO Check if we should return this */, true);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while finding commit timestamp", e);
        } catch (ExecutionException e) {
            throw new IOException("Problem finding commit timestamp", e);
        }

    }

    /**
     * @see java.io.Closeable#close()
     */
    @Override
    public final void close() throws IOException {

        if (ownsTSOClient) {
            tsoClient.close();
        }
        if (ownsCommitTableClient) {
            commitTableClient.close();
        }

    }

    // ****************************************************************************************************************
    // Helper methods
    // ****************************************************************************************************************

    private void enforceTransactionIsInRunningState(Transaction transaction) {

        if (transaction.getStatus() != Status.RUNNING) {
            throw new IllegalArgumentException("Transaction was already " + transaction.getStatus());
        }

    }

    @SuppressWarnings("unchecked")
    // NOTE: We are sure that tx is not parametrized
    private AbstractTransaction<? extends CellId>
            enforceAbstractTransactionAsParam(Transaction tx) {

        if (tx instanceof AbstractTransaction) {
            return (AbstractTransaction<? extends CellId>) tx;
        } else {
            throw new IllegalArgumentException(
                    "The transaction object passed is not an instance of AbstractTransaction");
        }

    }

    private void completeCommittedTx(AbstractTransaction<? extends CellId> txToSetup,
                                     long commitTS,
                                     boolean shouldRemoveCommitTableEntry)
            throws InterruptedException, ExecutionException
    {

        txToSetup.setStatus(Status.COMMITTED);
        txToSetup.setCommitTimestamp(commitTS);
        try {
            long startTimeInNs = System.nanoTime();
            updateShadowCells(txToSetup);
            shadowCellsUpdateTimer.update(System.nanoTime() - startTimeInNs);
            if (shouldRemoveCommitTableEntry) {
                // Remove transaction from commit table if not failure occurred
                startTimeInNs = System.nanoTime();
                commitTableClient.completeTransaction(txToSetup.getStartTimestamp()).get();
                commitTableUpdateTimer.update(System.nanoTime() - startTimeInNs);
            }
            postCommit(txToSetup);
        } catch (TransactionManagerException e) {
            LOG.warn(e.getMessage());
        }

    }

}
