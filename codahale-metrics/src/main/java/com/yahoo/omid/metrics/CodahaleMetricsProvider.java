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
package com.yahoo.omid.metrics;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.Timer.Context;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.google.common.base.Strings;
import com.google.common.net.HostAndPort;
import com.yahoo.omid.metrics.CodahaleMetricsConfig.Reporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;

public class CodahaleMetricsProvider implements MetricsProvider, MetricsRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(CodahaleMetricsProvider.class);

    public static final Pattern CODAHALE_METRICS_CONFIG_PATTERN = Pattern
        .compile(
            "(csv|slf4j|console|graphite):(.+):(\\d+):(DAYS|HOURS|MICROSECONDS|MILLISECONDS|MINUTES|NANOSECONDS|SECONDS)");

    private MetricRegistry metrics = new MetricRegistry();
    private List<ScheduledReporter> reporters = new ArrayList<>();

    private final int metricsOutputFrequency;
    private final TimeUnit timeUnit;

    public static CodahaleMetricsProvider createCodahaleMetricsProvider(List<String> metricsConfigs) {

        CodahaleMetricsConfig codahaleConfig = new CodahaleMetricsConfig();

        for (String metricConfig : metricsConfigs) {
            Matcher matcher = CODAHALE_METRICS_CONFIG_PATTERN.matcher(metricConfig);
            if (matcher.matches()) {

                String reporter = matcher.group(1);
                String reporterConfig = matcher.group(2);
                codahaleConfig.setOutputFreq(Integer.valueOf(matcher.group(3)));
                codahaleConfig.setOutputFreqTimeUnit(TimeUnit.valueOf(matcher.group(4)));

                switch(reporter) {
                    case "jmx":
                        codahaleConfig.addReporter(Reporter.JMX);
                        break;
                    case "csv":
                        codahaleConfig.addReporter(Reporter.CSV);
                        codahaleConfig.setCSVDir(reporterConfig);
                        break;
                    case "slf4j":
                        codahaleConfig.addReporter(Reporter.SLF4J);
                        codahaleConfig.setSlf4jLogger(reporterConfig);
                        break;
                    case "graphite":
                        codahaleConfig.addReporter(Reporter.GRAPHITE);
                        codahaleConfig.setGraphiteHostConfig(reporterConfig);
                        break;
                    case "console":
                        codahaleConfig.addReporter(Reporter.CONSOLE);
                        break;
                    default:
                        LOG.warn("Reporter {} unknown", reporter);
                        break;
                }
            } else {
                LOG.error("Pattern {} not recognized", metricConfig);
            }
        }
        CodahaleMetricsProvider provider = new CodahaleMetricsProvider(codahaleConfig);
        provider.startMetrics();
        return provider;
    }

    CodahaleMetricsProvider(CodahaleMetricsConfig conf) {
        metricsOutputFrequency = conf.getOutputFreq();
        timeUnit = conf.getOutputFreqTimeUnit();
        int reporterCount = 0;
        for (Reporter reporter : conf.getReporters()) {
            ScheduledReporter codahaleReporter = null;
            switch (reporter) {
                case CONSOLE:
                    codahaleReporter = createAndGetConfiguredConsoleReporter();
                    break;
                case GRAPHITE:
                    codahaleReporter = createAndGetConfiguredGraphiteReporter(conf.getPrefix(),
                                                                              conf.getGraphiteHostConfig());
                    break;
                case CSV:
                    codahaleReporter = createAndGetConfiguredCSVReporter(conf.getPrefix(),
                                                                         conf.getCSVDir());
                    break;
                case SLF4J:
                    codahaleReporter = createAndGetConfiguredSlf4jReporter(conf.getSlf4jLogger());
                    break;
            }
            if (codahaleReporter != null) {
                reporters.add(codahaleReporter);
                reporterCount++;
            }
        }
        if (reporterCount == 0) {
            LOG.warn("No metric reporters found, so metrics won't be available");
        }
    }

    @Override
    public void startMetrics() {
        for (ScheduledReporter r : reporters) {
            LOG.info("Starting reporter {} with freq {} {}",
                     r.getClass().getCanonicalName(), metricsOutputFrequency, timeUnit);
            r.start(metricsOutputFrequency, timeUnit);
        }
    }

    @Override
    public void stopMetrics() {
        for (ScheduledReporter r : reporters) {
            r.report();
            LOG.info("Stopping reporter {}", r.toString());
            r.stop();
        }
    }

    @Override
    public <T extends Number> void gauge(String name, Gauge<T> appGauge) {
        metrics.register(name, new CodahaleGauge<>(appGauge));
    }

    @Override
    public Counter counter(String name) {
        com.codahale.metrics.Counter counter = metrics.counter(name);
        return new CodahaleCounterWrapper(counter);
    }


    @Override
    public Timer timer(String name) {
        com.codahale.metrics.Timer timer = metrics.timer(name);
        return new CodahaleTimerWrapper(timer);
    }

    @Override
    public Meter meter(String name) {
        com.codahale.metrics.Meter meter = metrics.meter(name);
        return new CodahaleMeterWrapper(meter);
    }

    @Override
    public Histogram histogram(String name) {
        com.codahale.metrics.Histogram histogram = metrics.histogram(name);
        return new CodahaleHistogramWrapper(histogram);
    }

    private ScheduledReporter createAndGetConfiguredConsoleReporter() {
        return ConsoleReporter.forRegistry(metrics)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();
    }

    private ScheduledReporter createAndGetConfiguredGraphiteReporter(String prefix, String graphiteHost) {
        LOG.info("Configuring Graphite reporter. Sendig data to host:port {}", graphiteHost);
        HostAndPort addr = HostAndPort.fromString(graphiteHost);

        final Graphite graphite = new Graphite(
            new InetSocketAddress(addr.getHostText(), addr.getPort()));

        return GraphiteReporter.forRegistry(metrics)
            .prefixedWith(prefix)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .filter(MetricFilter.ALL)
            .build(graphite);
    }

    private ScheduledReporter createAndGetConfiguredCSVReporter(String prefix, String csvDir) {
        // NOTE:
        // 1) metrics output files are exclusive to a given process
        // 2) the output directory must exist
        // 3) if output files already exist they are not overwritten and there is no metrics output
        File outputDir;
        if (Strings.isNullOrEmpty(prefix)) {
            outputDir = new File(csvDir, prefix);
        } else {
            outputDir = new File(csvDir);
        }
        LOG.info("Configuring stats with csv output to directory [{}]", outputDir.getAbsolutePath());
        return CsvReporter.forRegistry(metrics)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build(outputDir);
    }


    private ScheduledReporter createAndGetConfiguredSlf4jReporter(String slf4jLogger) {
        LOG.info("Configuring stats with SLF4J with logger {}", slf4jLogger);
        return Slf4jReporter.forRegistry(metrics)
            .outputTo(LoggerFactory.getLogger(slf4jLogger))
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();
    }

    /**
     * Metrics wrapper implementations
     */

    private class CodahaleGauge<T extends Number> implements com.codahale.metrics.Gauge<T> {

        private final Gauge<T> omidGauge;

        CodahaleGauge(Gauge<T> omidGauge) {
            this.omidGauge = omidGauge;
        }

        @Override
        public T getValue() {
            return omidGauge.getValue();
        }

    }

    private class CodahaleCounterWrapper implements Counter {

        private final com.codahale.metrics.Counter counter;

        CodahaleCounterWrapper(com.codahale.metrics.Counter counter) {
            this.counter = counter;
        }

        @Override
        public void inc() {
            counter.inc();
        }

        @Override
        public void inc(long n) {
            counter.inc(n);
        }

        @Override
        public void dec() {
            counter.dec();
        }

        @Override
        public void dec(long n) {
            counter.dec(n);
        }

    }

    private class CodahaleTimerWrapper implements Timer {

        private final com.codahale.metrics.Timer timer;

        private Context context;

        CodahaleTimerWrapper(com.codahale.metrics.Timer timer) {
            this.timer = timer;
        }

        @Override
        public void start() {
            context = timer.time();
        }

        @Override
        public void stop() {
            context.stop();
        }

        @Override
        public void update(long durationInNs) {
            timer.update(durationInNs, TimeUnit.NANOSECONDS);
        }

    }

    private class CodahaleMeterWrapper implements Meter {

        private com.codahale.metrics.Meter meter;

        CodahaleMeterWrapper(com.codahale.metrics.Meter meter) {
            this.meter = meter;
        }

        @Override
        public void mark() {
            meter.mark();
        }

        @Override
        public void mark(long n) {
            meter.mark(n);
        }

    }

    private class CodahaleHistogramWrapper implements Histogram {

        private com.codahale.metrics.Histogram histogram;

        CodahaleHistogramWrapper(com.codahale.metrics.Histogram histogram) {
            this.histogram = histogram;
        }

        @Override
        public void update(int value) {
            histogram.update(value);
        }

        @Override
        public void update(long value) {
            histogram.update(value);
        }

    }

}