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
import com.yahoo.omid.tsoclient.AbortException;
import com.yahoo.omid.tsoclient.ConnectionException;
import com.yahoo.omid.tsoclient.NewTSOException;
import com.yahoo.omid.tsoclient.ServiceUnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static com.yahoo.omid.committable.CommitTable.CommitTimestamp.Location.CACHE;
import static com.yahoo.omid.committable.CommitTable.CommitTimestamp.Location.COMMIT_TABLE;
import static com.yahoo.omid.committable.CommitTable.CommitTimestamp.Location.NOT_PRESENT;
import static com.yahoo.omid.committable.CommitTable.CommitTimestamp.Location.SHADOW_CELL;
import static com.yahoo.omid.metrics.MetricsUtils.name;

/**
 * Omid's base abstract implementation of the {@link TransactionManager} interface.
 *
 * Provides extra methods to allow transaction manager developers to perform
 * different actions before/after the methods exposed by the {@link TransactionManager} interface.
 *
 * So, this abstract class must be extended by particular implementations of
 * transaction managers related to different storage systems (HBase...)
 */
public abstract class AbstractTransactionManager implements TransactionManager {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractTransactionManager.class);

    public interface TransactionFactory<T extends CellId> {

        AbstractTransaction<T> createTransaction(long transactionId, long epoch, AbstractTransactionManager tm);

    }

    protected final TSOClient tsoClient;
    protected final CommitTable.Client commitTableClient;
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
     * @param commitTableClient
     *            a client for accessing functionality of the commit table
     * @param transactionFactory
     *            a transaction factory to create the specific transaction
     *            objects required by the transaction manager being implemented.
     */
    public AbstractTransactionManager(MetricsRegistry metrics,
                                      TSOClient tsoClient,
                                      CommitTable.Client commitTableClient,
                                      TransactionFactory<? extends CellId> transactionFactory) {
        this.tsoClient = tsoClient;
        this.commitTableClient = commitTableClient;
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
     * Allows transaction manager developers to perform actions before creating a transaction.
     * @throws TransactionManagerException
     */
    public void preBegin() throws TransactionManagerException {}

    /**
     * @see com.yahoo.omid.transaction.TransactionManager#begin()
     */
    @Override
    public final Transaction begin() throws TransactionException {

        try {
            preBegin();

            long startTimestamp, epoch;

            // The loop is required for HA scenarios where we get the timestamp
            // but when getting the epoch, the client is connected to a new TSOServer
            // When this happen, the epoch will be larger than the startTimestamp,
            // so we need to start the transaction again. We use the fact that epoch
            // is always smaller or equal to a timestamp, and therefore, we first need
            // to get the timestamp and then the epoch.
            startTimestampTimer.start();
            try {
                do {
                    startTimestamp = tsoClient.getNewStartTimestamp().get();
                    epoch = tsoClient.getEpoch();
                } while (epoch > startTimestamp);
            } finally {
                startTimestampTimer.stop();
            }

            AbstractTransaction<? extends CellId> tx = transactionFactory.createTransaction(startTimestamp, epoch, this);

            postBegin(tx);

            return tx;
        } catch (TransactionManagerException e) {
            throw new TransactionException("An error has occured during PreBegin/PostBegin", e);
        } catch (ExecutionException e) {
            throw new TransactionException("Could not get new timestamp", e);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new TransactionException("Interrupted getting timestamp", ie);
        }
    }

    /**
     * Allows transaction manager developers to perform actions after having started a transaction.
     * @param transaction
     *            the transaction that was just created.
     * @throws TransactionManagerException
     */
    public void postBegin(AbstractTransaction<? extends CellId> transaction) throws TransactionManagerException {}

    /**
     * Allows transaction manager developers to perform actions before committing a transaction.
     * @param transaction
     *            the transaction that is going to be committed.
     * @throws TransactionManagerException
     */
    public void preCommit(AbstractTransaction<? extends CellId> transaction) throws TransactionManagerException {}

    /**
     * @see com.yahoo.omid.transaction.TransactionManager#commit(Transaction)
     */
    @Override
    public final void commit(Transaction transaction) throws RollbackException, TransactionException {

        AbstractTransaction<? extends CellId> tx = enforceAbstractTransactionAsParam(transaction);
        enforceTransactionIsInRunningState(tx);

        if (tx.isRollbackOnly()) { // Manage explicit user rollback
            rollback(tx);
            throw new RollbackException(tx + ": Tx was set to rollback explicitly");
        }

        try {

            preCommit(tx);

            commitTimer.start();
            try {
                commitRegularTransaction(tx);
                committedTxsCounter.inc();
            } finally {
                commitTimer.stop();
            }

            postCommit(tx);

        } catch (TransactionManagerException e) {
            throw new TransactionException(e.getMessage(), e);
        }

    }

    /**
     * Allows transaction manager developers to perform actions after committing a transaction.
     * @param transaction
     *            the transaction that was committed.
     * @throws TransactionManagerException
     */
    public void postCommit(AbstractTransaction<? extends CellId> transaction) throws TransactionManagerException {}

    /**
     * Allows transaction manager developers to perform actions before rolling-back a transaction.
     * @param transaction
     *            the transaction that is going to be rolled-back.
     * @throws TransactionManagerException
     */
    public void preRollback(AbstractTransaction<? extends CellId> transaction) throws TransactionManagerException {}

    /**
     * @see com.yahoo.omid.transaction.TransactionManager#rollback(Transaction)
     */
    @Override
    public final void rollback(Transaction transaction) throws TransactionException {

        AbstractTransaction<? extends CellId> tx = enforceAbstractTransactionAsParam(transaction);
        enforceTransactionIsInRunningState(tx);

        try {

            preRollback(tx);

            // Make sure its commit timestamp is 0, so the cleanup does the right job
            tx.setCommitTimestamp(0);
            tx.setStatus(Status.ROLLEDBACK);

            postRollback(tx);

        } catch (TransactionManagerException e) {
            throw new TransactionException(e.getMessage(), e);
        } finally {
            tx.cleanup();
        }

    }

    /**
     * Allows transaction manager developers to perform actions after rolling-back a transaction.
     * @param transaction
     *            the transaction that was rolled-back.
     * @throws TransactionManagerException
     */
    public void postRollback(AbstractTransaction<? extends CellId> transaction) throws TransactionManagerException {}

    /**
     * Check if the transaction commit data is in the shadow cell
     * @param cellStartTimestamp
     *            the transaction start timestamp
     *        locator
     *            the timestamp locator
     * @throws IOException
     */
    Optional<CommitTimestamp> readCommitTimestampFromShadowCell(long cellStartTimestamp, CommitTimestampLocator locator)
            throws IOException
    {

        Optional<CommitTimestamp> commitTS = Optional.absent();

        Optional<Long> commitTimestamp = locator.readCommitTimestampFromShadowCell(cellStartTimestamp);
        if (commitTimestamp.isPresent()) {
            commitTS = Optional.of(new CommitTimestamp(SHADOW_CELL, commitTimestamp.get(), true)); // Valid commit TS
        }

        return commitTS;
    }

    /**
     * This function returns the commit timestamp for a particular cell if the transaction was already committed in
     * the system. In case the transaction was not committed and the cell was written by transaction initialized by a
     * previous TSO server, an invalidation try occurs.
     * Otherwise the function returns a value that indicates that the commit timestamp was not found.
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
            Optional<Long> commitTimestamp = locator.readCommitTimestampFromCache(cellStartTimestamp);
            if (commitTimestamp.isPresent()) { // Valid commit timestamp
                return new CommitTimestamp(CACHE, commitTimestamp.get(), true);
            }

            // 2) Then check the commit table
            // If the data was written at a previous epoch, check whether the transaction was invalidated
            Optional<CommitTimestamp> commitTimeStamp = commitTableClient.getCommitTimestamp(cellStartTimestamp).get();
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

            // 5) We did not manage to invalidate the transactions then check the commit table
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

        tsoClient.close();
        commitTableClient.close();

    }

    // ----------------------------------------------------------------------------------------------------------------
    // Helper methods
    // ----------------------------------------------------------------------------------------------------------------

    private void enforceTransactionIsInRunningState(Transaction transaction) {

        if (transaction.getStatus() != Status.RUNNING) {
            throw new IllegalArgumentException("Transaction was already " + transaction.getStatus());
        }

    }

    @SuppressWarnings("unchecked")
    // NOTE: We are sure that tx is not parametrized
    private AbstractTransaction<? extends CellId> enforceAbstractTransactionAsParam(Transaction tx) {

        if (tx instanceof AbstractTransaction) {
            return (AbstractTransaction<? extends CellId>) tx;
        } else {
            throw new IllegalArgumentException(
                    "The transaction object passed is not an instance of AbstractTransaction");
        }

    }

    private void commitRegularTransaction(AbstractTransaction<? extends CellId> tx)
            throws RollbackException, TransactionException, TransactionManagerException
    {

        try {
            long commitTs = tsoClient.commit(tx.getStartTimestamp(), tx.getWriteSet()).get();
            certifyCommitForTx(tx, commitTs);
            updateShadowCellsForTx(tx);
            removeCommitTableEntryForTx(tx);
        } catch (ExecutionException e) {

            if (e.getCause() instanceof AbortException) { // TSO reports Tx conflicts as AbortExceptions in the future
                rollback(tx);
                rolledbackTxsCounter.inc();
                throw new RollbackException("Conflicts detected in tx writeset", e.getCause());
            }

            if (e.getCause() instanceof ServiceUnavailableException
                    ||
                    e.getCause() instanceof NewTSOException
                    ||
                    e.getCause() instanceof ConnectionException) {

                errorTxsCounter.inc();
                try {
                    LOG.warn("Can't contact the TSO for receiving outcome for Tx {}. Checking Commit Table...", tx);
                    // Check the commit table to find if the target TSO woke up in the meantime and added the commit
                    // TODO: Decide what we should we do if we can not contact the commit table
                    Optional<CommitTimestamp> commitTimestamp =
                            commitTableClient.getCommitTimestamp(tx.getStartTimestamp()).get();
                    if (commitTimestamp.isPresent()) {
                        if (commitTimestamp.get().isValid()) {
                            LOG.warn("{}: Valid commit TS found in Commit Table. Committing Tx...", tx);
                            certifyCommitForTx(tx, commitTimestamp.get().getValue());
                            updateShadowCellsForTx(tx); // But do NOT remove transaction from commit table
                        } else { // Probably another Tx in a new TSO Server invalidated this transaction
                            LOG.warn("{}: Invalidated commit TS found in Commit Table. Rolling-back...", tx);
                            rollback(tx);
                            throw new RollbackException(tx + " invalidated by other Tx started", e.getCause());
                        }
                    } else {
                        LOG.warn("{}: Trying to invalidate Tx proactively in Commit Table...", tx);
                        boolean invalidated = commitTableClient.tryInvalidateTransaction(tx.getStartTimestamp()).get();
                        if (invalidated) {
                            LOG.warn("{}: Invalidated proactively in Commit Table. Rolling-back Tx...", tx);
                            invalidatedTxsCounter.inc();
                            rollback(tx); // Rollback proactively cause it's likely that a new TSOServer is now master
                            throw new RollbackException(tx + " rolled-back precautionary", e.getCause());
                        } else {
                            LOG.warn("{}: Invalidation could NOT be completed. Re-checking Commit Table...", tx);
                            // TODO: Decide what we should we do if we can not contact the commit table
                            commitTimestamp = commitTableClient.getCommitTimestamp(tx.getStartTimestamp()).get();
                            if (commitTimestamp.isPresent() && commitTimestamp.get().isValid()) {
                                LOG.warn("{}: Valid commit TS found in Commit Table. Committing Tx...", tx);
                                certifyCommitForTx(tx, commitTimestamp.get().getValue());
                                updateShadowCellsForTx(tx); // But do NOT remove transaction from commit table
                            } else {
                                LOG.error("{}: Can't determine Transaction outcome", tx);
                                throw new TransactionException(tx + ": cannot determine Tx outcome");
                            }
                        }
                    }
                } catch (ExecutionException e1) {
                    throw new TransactionException(tx + ": problem reading commitTS from Commit Table", e1);
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                    throw new TransactionException(tx + ": interrupted while reading commitTS from Commit Table", e1);
                }
            } else {
                throw new TransactionException(tx + ": cannot determine Tx outcome", e.getCause());
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new TransactionException(tx + ": interrupted during commit", ie);
        }

    }

    private void certifyCommitForTx(AbstractTransaction<? extends CellId> txToSetup, long commitTS) {

        txToSetup.setStatus(Status.COMMITTED);
        txToSetup.setCommitTimestamp(commitTS);

    }

    private void updateShadowCellsForTx(AbstractTransaction<? extends CellId> tx) throws TransactionManagerException {

        shadowCellsUpdateTimer.start();
        try {
            updateShadowCells(tx);
        } finally {
            shadowCellsUpdateTimer.stop();
        }

    }

    private void removeCommitTableEntryForTx(AbstractTransaction<? extends CellId> tx) throws TransactionManagerException {

        commitTableUpdateTimer.start();
        try {
            commitTableClient.completeTransaction(tx.getStartTimestamp()).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TransactionManagerException(tx + ": interrupted during commit table entry delete");
        } catch (ExecutionException e) {
            throw new TransactionManagerException(tx + ": can't remove commit table entry");
        } finally {
            commitTableUpdateTimer.stop();
        }

    }

}
