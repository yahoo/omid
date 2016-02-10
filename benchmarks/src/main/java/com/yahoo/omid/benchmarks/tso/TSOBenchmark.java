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

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.yahoo.omid.benchmarks.utils.GeneratorUtils;
import com.yahoo.omid.benchmarks.utils.GeneratorUtils.RowDistribution;
import com.yahoo.omid.committable.hbase.CommitTableConstants;
import com.yahoo.omid.metrics.CodahaleMetricsProvider;
import com.yahoo.omid.tools.hbase.HBaseLogin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;

import static com.yahoo.omid.metrics.CodahaleMetricsProvider.CODAHALE_METRICS_CONFIG_PATTERN;

/**
 * Benchmark using directly TSOClient
 */
public class TSOBenchmark implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(TSOBenchmark.class);

    private volatile boolean isCleaningDone = false;

    private final Config expConfig;

    // Clients triggering txs (threads) & corresponding executor
    private final ArrayList<TxRunner> txRunners = new ArrayList<>();
    private final ScheduledExecutorService txRunnerExec;

    private final CodahaleMetricsProvider metrics;

    public TSOBenchmark(Config expConfig) {

        this.expConfig = expConfig;

        this.metrics = CodahaleMetricsProvider.createCodahaleMetricsProvider(
                Arrays.asList(expConfig.metricsConfig.toString().split("\\s*,\\s*")));

        // Executor for TxRunners (Clients triggering transactions)
        this.txRunnerExec = Executors.newScheduledThreadPool(expConfig.nbClients,
                new ThreadFactoryBuilder().setNameFormat("tx-runner-%d")
                        .setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                            @Override
                            public void uncaughtException(Thread t, Throwable e) {
                                LOG.error("Thread {} threw exception", t, e);
                            }
                        }).build());

    }

    public void createTxRunner() throws IOException, InterruptedException, ExecutionException {

        TxRunner txRunner = new TxRunner(metrics, expConfig);
        txRunnerExec.submit(txRunner);

        txRunners.add(txRunner);

    }

    @Override
    public void close() {

        // Stop clients
        for (TxRunner txRunner : txRunners) {
            txRunner.stop();
        }

        // Shutdown executor
        try {
            LOG.info("Closing TxRunner Executor in {} 30 secs");
            boolean wasSuccess = txRunnerExec.awaitTermination(30, TimeUnit.SECONDS);
            if (!wasSuccess) {
                txRunnerExec.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.info("Interrupted whilst shutting down TxRunner Executor!");
        } finally {
            LOG.info("TxRunner Executor stopped");
        }

        // Close metrics
        metrics.stopMetrics();
        LOG.info("Metrics stopped");

        isCleaningDone = true;
    }

    public void attachShutDownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread("benchmark-cleaner") {
            @Override
            public void run() {
                if (!isCleaningDone) {
                    close();
                }
            }
        });
        LOG.info("Shutdown Hook Attached");
    }

    public static void main(String[] args) throws Exception {

        final Config config = Config.parseConfig(args);

        final int nOfTxRunners = config.nbClients;

        final int benchmarkTimeValue = config.runFor.timeValue;
        final TimeUnit benchmarkTimeUnit = config.runFor.timeUnit;

        try (TSOBenchmark tsoBenchmark = new TSOBenchmark(config)) {

            tsoBenchmark.attachShutDownHook();

            LOG.info("----- Starting TSO Benchmark [ {} TxRunner clients ] -----", nOfTxRunners);

            for (int i = 0; i < nOfTxRunners; ++i) {
                tsoBenchmark.createTxRunner();
            }

            LOG.info("Benchmark run lenght {} {}", benchmarkTimeValue, benchmarkTimeUnit);
            benchmarkTimeUnit.sleep(benchmarkTimeValue);

        } finally {
            LOG.info("----- TSO Benchmark complete - Check metrics from individual clients in log -----");
        }

    }

    static class Config {

        static public Config parseConfig(String args[]) {
            Config config = new Config();
            new JCommander(config, args);
            return config;
        }

        @Parameter(names = "-nOfClients", description = "Number of TSO clients (TxRunners)")
        int nbClients = 1;

        @Parameter(names = "-tsoHostAndPort",
                   description = "Hostname and port of the TSO. Format: HOST:PORT",
                   converter = HostPortConverter.class)
        HostAndPort tsoHostPort = HostAndPort.fromParts("localhost", 54758);

        @Parameter(names = "-runFor",
                   converter = TimeValueTimeUnitConverter.class,
                   description = "Benchmark run lenght.\n"
                               + "Format: TIME_VALUE:TIME_UNIT Example: 10:DAYS, 60:SECONDS...")
        TimeValueTimeUnit runFor = new TimeValueTimeUnit(10, TimeUnit.MINUTES);

        @Parameter(names = "-requestDistribution",
                   description = "Request distribution (how to pick rows) [ UNIFORM | ZIPFIAN ]",
                   converter = GeneratorUtils.RowDistributionConverter.class)
        RowDistribution requestDistribution = RowDistribution.UNIFORM;

        @Parameter(names = "-maxWritesetSize", description = "Maximum size of tx in terms of modified columns,\n"
                                                           + "homogeneously distributed between 1 & this number")
        int maxTxSize = TxRunner.DEFAULT_WRITESET_SIZE;

        @Parameter(names = "-maxInFlight", description = "Max number of outstanding messages in the TSO pipe")
        int maxInFlight = 100_000;

        @Parameter(names = "-commitDelay",
                   converter = TimeValueTimeUnitConverter.class,
                   description = "Simulated delay between acquiring timestamp and committing.\n"
                               + "Format: TIME_VALUE:TIME_UNIT Example: 50:MILLISECONDS...")
        TimeValueTimeUnit commitDelay = new TimeValueTimeUnit(50, TimeUnit.MILLISECONDS);

        @Parameter(names = "-percentRead", description = "% reads")
        float percentReads = 0;

        @Parameter(names = "-readProportion",
                   description = "Proportion of reads, between 1 and 0.\n"
                               + "Overrides -percentRead if specified",
                   hidden = true)
        float readproportion = -1;

        @Parameter(names = "-useHBase", description = "Enable HBase storage")
        private boolean hbase = false;

        @Parameter(names = "-commitTableNameHBase", description = "HBase commit table name")
        private String hbaseCommitTable = CommitTableConstants.COMMIT_TABLE_DEFAULT_NAME;

        @Parameter(names = "-metricsConfig",
                   converter = MetricsConfigConverter.class,
                   description = "Format: REPORTER:REPORTER_CONFIG:TIME_VALUE:TIME_UNIT")
        MetricsConfig metricsConfig = new MetricsConfig("console", "", 10, TimeUnit.SECONDS);

        @ParametersDelegate
        private HBaseLogin.Config loginFlags = new HBaseLogin.Config();

        public boolean isHBase() {
            return hbase;
        }

        public String getHBaseCommitTable() {
            return hbaseCommitTable;
        }

        public HBaseLogin.Config getLoginFlags() {
            return loginFlags;
        }
    }

    public static class MetricsConfig {

        public String reporter;
        public String reporterConfig;
        public Integer timeValue;
        public TimeUnit timeUnit;

        public MetricsConfig(String reporter, String reporterConfig, Integer timeValue, TimeUnit timeUnit) {
            this.reporter = reporter;
            this.reporterConfig = reporterConfig;
            this.timeValue = timeValue;
            this.timeUnit = timeUnit;
        }

        @Override
        public String toString() {
            return reporter + ":" + reporterConfig + ":" + timeValue + ":" + timeUnit;
        }

    }

    public static class MetricsConfigConverter implements IStringConverter<MetricsConfig> {

        @Override
        public MetricsConfig convert(String value) {

            Matcher matcher = CODAHALE_METRICS_CONFIG_PATTERN.matcher(value);

            if (matcher.matches()) {
                return new MetricsConfig(matcher.group(1),
                                         matcher.group(2),
                                         Integer.valueOf(matcher.group(3)),
                                         TimeUnit.valueOf(matcher.group(4)));
            } else {
                throw new ParameterException("Metrics specification " + value
                        + " should have this format: REPORTER:REPORTER_CONFIG:TIME_VALUE:TIME_UNIT\n"
                        + " where REPORTER=csv|slf4j|console|graphite");
            }

        }

    }

    public static class TimeValueTimeUnit {

        public Integer timeValue;
        public TimeUnit timeUnit;

        public TimeValueTimeUnit(Integer timeValue, TimeUnit timeUnit) {
            this.timeValue = timeValue;
            this.timeUnit = timeUnit;
        }

    }

    public static class TimeValueTimeUnitConverter implements IStringConverter<TimeValueTimeUnit> {
        @Override
        public TimeValueTimeUnit convert(String value) {
            String[] s = value.split(":");
            try {
                return new TimeValueTimeUnit(Integer.parseInt(s[0]), TimeUnit.valueOf(s[1].toUpperCase()));
            } catch(Exception e) {
                throw new ParameterException("Time specification " + value +
                        " should have this format: TIME_VALUE:TIME_UNIT Example: 10:DAYS, 60:SECONDS...");
            }
        }
    }

    public static class HostPortConverter implements IStringConverter<HostAndPort> {

        @Override
        public HostAndPort convert(String value) {
            return HostAndPort.fromString(value);
        }

    }

}
