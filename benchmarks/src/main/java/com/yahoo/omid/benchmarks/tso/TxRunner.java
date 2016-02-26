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
package com.yahoo.omid.benchmarks.tso;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.yahoo.omid.benchmarks.tso.TSOBenchmark.Config;
import com.yahoo.omid.benchmarks.tso.TSOBenchmark.TimeValueTimeUnit;
import com.yahoo.omid.benchmarks.utils.GeneratorUtils;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.NullCommitTable;
import com.yahoo.omid.committable.hbase.HBaseCommitTable;
import com.yahoo.omid.committable.hbase.HBaseCommitTableConfig;
import com.yahoo.omid.metrics.Counter;
import com.yahoo.omid.metrics.MetricsRegistry;
import com.yahoo.omid.metrics.Timer;
import com.yahoo.omid.tools.hbase.HBaseLogin;
import com.yahoo.omid.tso.util.DummyCellIdImpl;
import com.yahoo.omid.tsoclient.CellId;
import com.yahoo.omid.tsoclient.OmidClientConfiguration;
import com.yahoo.omid.tsoclient.TSOClient;
import com.yahoo.omid.tsoclient.TSOFuture;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;

public class TxRunner implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(TxRunner.class);

    private static volatile int txRunnerCounter = 0;
    private int txRunnerId = txRunnerCounter++;

    private final ScheduledExecutorService callbackExec = Executors.newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setNameFormat("tx-runner-" + txRunnerId + "-callback")
                    .setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                        @Override
                        public void uncaughtException(Thread t, Throwable e) {
                            LOG.error("Thread {} threw exception", t, e);
                        }
                    }).build());

    /**
     * Maximum number of modified rows in each transaction
     */
    public static final int DEFAULT_WRITESET_SIZE = 20;

    private volatile boolean isRunning = false;

    private final int maxTxSize;
    private final Semaphore outstandingTxs;
    private final TimeValueTimeUnit commitDelay;
    private final float percentReads;
    private IntegerGenerator intGenerator;
    private final Random randomGen;

    // Main elements
    private final TSOClient tsoClient;
    private final CommitTable.Client commitTableClient;

    // Statistics to save
    private final Timer timestampTimer;
    private final Timer commitTimer;
    private final Timer abortTimer;
    private final Counter errorCounter;

    public TxRunner(MetricsRegistry metrics, Config expConfig) throws IOException, InterruptedException,
            ExecutionException {

        // Tx Runner config
        this.outstandingTxs = new Semaphore(expConfig.maxInFlight);
        this.maxTxSize = expConfig.maxTxSize;
        this.commitDelay = expConfig.commitDelay;
        this.percentReads = expConfig.readproportion == -1 ? expConfig.percentReads : (expConfig.readproportion * 100);
        this.intGenerator = GeneratorUtils.getIntGenerator(expConfig.requestDistribution);
        this.randomGen = new Random(System.currentTimeMillis() * txRunnerId); // to make it channel dependent

        LOG.info("TxRunner-{} [ Row Distribution -> {} ]", txRunnerId, expConfig.requestDistribution);
        LOG.info("TxRunner-{} [ Outstanding transactions -> {} ]", txRunnerId, expConfig.maxInFlight);
        LOG.info("TxRunner-{} [ Max Tx Size -> {} ]", txRunnerId, expConfig.maxTxSize);
        LOG.info("TxRunner-{} [ Commit delay -> {}:{} ]", txRunnerId,
                expConfig.commitDelay.timeValue,
                expConfig.commitDelay.timeUnit);
        LOG.info("TxRunner-{} [ % reads -> {} ]", txRunnerId, expConfig.percentReads);

        // Commit table client initialization
        CommitTable commitTable;
        if (expConfig.isHBase()) {
            HBaseLogin.loginIfNeeded(expConfig.getLoginFlags());
            HBaseCommitTableConfig hbaseCommitTableConfig = new HBaseCommitTableConfig();
            hbaseCommitTableConfig.setTableName(expConfig.getHBaseCommitTable());
            commitTable = new HBaseCommitTable(HBaseConfiguration.create(), hbaseCommitTableConfig);
            LOG.info("TxRunner-{} [ Using HBase Commit Table ]", txRunnerId);
        } else {
            commitTable = new NullCommitTable();
            LOG.info("TxRunner-{} [ Using Null Commit Table ]", txRunnerId);
        }
        try {
            this.commitTableClient = commitTable.getClient().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
            throw e;
        } catch (ExecutionException e) {
            e.printStackTrace();
            throw e;
        }

        // TSO Client initialization
        LOG.info("TxRunner-{} [ Connected to TSO in {} ]", txRunnerId, expConfig.tsoHostPort);
        OmidClientConfiguration tsoClientConf = OmidClientConfiguration.create();
        tsoClientConf.setConnectionString(expConfig.tsoHostPort.toString());
        this.tsoClient = TSOClient.newInstance(tsoClientConf);

        // Stat initialization
        String hostName = InetAddress.getLocalHost().getHostName();
        this.timestampTimer = metrics.timer(name("tx_runner", Integer.toString(txRunnerId), hostName, "timestamp"));
        this.commitTimer = metrics.timer(name("tx_runner", Integer.toString(txRunnerId), hostName, "commit"));
        this.abortTimer = metrics.timer(name("tx_runner", Integer.toString(txRunnerId), hostName, "abort"));
        this.errorCounter = metrics.counter(name("tx_runner", Integer.toString(txRunnerId), hostName, "errors"));

    }

    @Override
    public void run() {
        isRunning = true;
        try {
            while (isRunning) {
                outstandingTxs.acquire();
                long tsRequestTime = System.nanoTime();
                final TSOFuture<Long> tsFuture = tsoClient.getNewStartTimestamp();
                tsFuture.addListener(new TimestampListener(tsFuture, tsRequestTime), callbackExec);
            }
        } catch (InterruptedException ie) {
            // normal, ignore, we're shutting down
            Thread.currentThread().interrupt();
            LOG.info("TxRunner {} interrupted! Finishing...", txRunnerId);
        } finally {
            shutdown();
        }
    }

    public void stop() {
        isRunning = false;
    }

    private void shutdown() {
        try {
            LOG.info("Finishing TxRunner {} in 10 secs", txRunnerId);
            boolean wasSuccess = callbackExec.awaitTermination(10, TimeUnit.SECONDS);
            if (!wasSuccess) {
                callbackExec.shutdownNow();
            }
            commitTableClient.close();
            tsoClient.close().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // ignore
        } catch (ExecutionException | IOException e) {
            // ignore
        } finally {
            LOG.info("TxRunner {} finished", txRunnerId);
        }
    }

    class TimestampListener implements Runnable {
        TSOFuture<Long> tsFuture;
        final long tsRequestTime;

        TimestampListener(TSOFuture<Long> tsFuture, long tsRequestTime) {
            this.tsFuture = tsFuture;
            this.tsRequestTime = tsRequestTime;
        }

        @Override
        public void run() {

            try {
                long txId = tsFuture.get();
                timestampTimer.update(System.nanoTime() - tsRequestTime);
                callbackExec.schedule(new DeferredCommitterListener(txId), commitDelay.timeValue, commitDelay.timeUnit);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                errorCounter.inc();
                outstandingTxs.release();
            } catch (ExecutionException e) {
                errorCounter.inc();
                outstandingTxs.release();
            }

        }
    }

    class DeferredCommitterListener implements Runnable {
        final long txId;

        DeferredCommitterListener(long txId) {
            this.txId = txId;
        }

        @Override
        public void run() {
            boolean readOnly = (randomGen.nextFloat() * 100) < percentReads;

            int txSize = readOnly ? 0 : randomGen.nextInt(maxTxSize);
            final Set<CellId> cells = new HashSet<CellId>();
            for (byte i = 0; i < txSize; i++) {
                long cellId = intGenerator.nextInt();
                cells.add(new DummyCellIdImpl(cellId));
            }
            final TSOFuture<Long> commitFuture = tsoClient.commit(txId, cells);
            commitFuture.addListener(new CommitListener(txId, commitFuture, System.nanoTime()), callbackExec);
        }
    }

    class CommitListener implements Runnable {
        final long txId;
        final long commitRequestTime;
        TSOFuture<Long> commitFuture;

        CommitListener(long txId, TSOFuture<Long> commitFuture, long commitRequestTime) {
            this.txId = txId;
            this.commitFuture = commitFuture;
            this.commitRequestTime = commitRequestTime;
        }

        @Override
        public void run() {

            try {
                commitFuture.get();
                commitTableClient.completeTransaction(txId);
                commitTimer.update(System.nanoTime() - commitRequestTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                errorCounter.inc();
            } catch (ExecutionException e) {
                abortTimer.update(System.nanoTime() - commitRequestTime);
            } finally {
                outstandingTxs.release();
            }

        }
    }

}
