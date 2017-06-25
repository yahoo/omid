/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.omid.tso;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.name.Named;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;

import org.apache.commons.pool2.ObjectPool;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.metrics.MetricsRegistry;
import org.apache.omid.metrics.Timer;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.lmax.disruptor.dsl.ProducerType.SINGLE;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.omid.metrics.MetricsUtils.name;
import static org.apache.omid.tso.PersistenceProcessorImpl.PersistBatchEvent.EVENT_FACTORY;
import static org.apache.omid.tso.PersistenceProcessorImpl.PersistBatchEvent.makePersistBatch;

class PersistenceProcessorImpl implements PersistenceProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(PersistenceProcessorImpl.class);

    // Disruptor-related attributes
    private final ExecutorService disruptorExec;
    private final Disruptor<PersistBatchEvent> disruptor;
    private final RingBuffer<PersistBatchEvent> persistRing;

    private final ObjectPool<Batch> batchPool;
    @VisibleForTesting
    Batch currentBatch;

    // TODO Next two need to be either int or AtomicLong
    volatile private long batchSequence;

    private CommitTable.Writer lowWatermarkWriter;
    private ExecutorService lowWatermarkWriterExecutor;

    private MetricsRegistry metrics;
    private final Timer lwmWriteTimer;

    @Inject
    PersistenceProcessorImpl(TSOServerConfig config,
                             @Named("PersistenceStrategy") WaitStrategy strategy,
                             CommitTable commitTable,
                             ObjectPool<Batch> batchPool,
                             Panicker panicker,
                             PersistenceProcessorHandler[] handlers,
                             MetricsRegistry metrics)
            throws Exception {

        // ------------------------------------------------------------------------------------------------------------
        // Disruptor initialization
        // ------------------------------------------------------------------------------------------------------------

        ThreadFactoryBuilder threadFactory = new ThreadFactoryBuilder().setNameFormat("persist-%d");
        this.disruptorExec = Executors.newFixedThreadPool(config.getNumConcurrentCTWriters(), threadFactory.build());

        this.disruptor = new Disruptor<>(EVENT_FACTORY, 1 << 20, disruptorExec , SINGLE, strategy);
        disruptor.handleExceptionsWith(new FatalExceptionHandler(panicker)); // This must be before handleEventsWith()
        disruptor.handleEventsWithWorkerPool(handlers);
        this.persistRing = disruptor.start();

        // ------------------------------------------------------------------------------------------------------------
        // Attribute initialization
        // ------------------------------------------------------------------------------------------------------------

        this.metrics = metrics;
        this.lowWatermarkWriter = commitTable.getWriter();
        this.batchSequence = 0L;
        this.batchPool = batchPool;
        this.currentBatch = batchPool.borrowObject();
        // Low Watermark writer
        ThreadFactoryBuilder lwmThreadFactory = new ThreadFactoryBuilder().setNameFormat("lwm-writer-%d");
        this.lowWatermarkWriterExecutor = Executors.newSingleThreadExecutor(lwmThreadFactory.build());

        // Metrics config
        this.lwmWriteTimer = metrics.timer(name("tso", "lwmWriter", "latency"));

        LOG.info("PersistentProcessor initialized");

    }

    @Override
    public void triggerCurrentBatchFlush() throws Exception {

        if (currentBatch.isEmpty()) {
            return;
        }
        long seq = persistRing.next();
        PersistBatchEvent e = persistRing.get(seq);
        makePersistBatch(e, batchSequence++, currentBatch);
        persistRing.publish(seq);
        currentBatch = batchPool.borrowObject();

    }

    @Override
    public void addCommitToBatch(long startTimestamp, long commitTimestamp, Channel c, MonitoringContext monCtx)
            throws Exception {

        currentBatch.addCommit(startTimestamp, commitTimestamp, c, monCtx);
        if (currentBatch.isFull()) {
            triggerCurrentBatchFlush();
        }

    }

    @Override
    public void addCommitRetryToBatch(long startTimestamp, Channel c, MonitoringContext monCtx) throws Exception {
        currentBatch.addCommitRetry(startTimestamp, c, monCtx);
        if (currentBatch.isFull()) {
            triggerCurrentBatchFlush();
        }
    }

    @Override
    public void addAbortToBatch(long startTimestamp, Channel c, MonitoringContext monCtx)
            throws Exception {

        currentBatch.addAbort(startTimestamp, c, monCtx);
        if (currentBatch.isFull()) {
            triggerCurrentBatchFlush();
        }

    }

    @Override
    public void addTimestampToBatch(long startTimestamp, Channel c, MonitoringContext monCtx) throws Exception {

        currentBatch.addTimestamp(startTimestamp, c, monCtx);
        if (currentBatch.isFull()) {
            triggerCurrentBatchFlush();
        }

    }

    @Override
    public void addFenceToBatch(long tableID, long fenceTimestamp, Channel c, MonitoringContext monCtx) throws Exception {

        currentBatch.addFence(tableID, fenceTimestamp, c, monCtx);
        if (currentBatch.isFull()) {
            triggerCurrentBatchFlush();
        }

    }

    @Override
    public Future<Void> persistLowWatermark(final long lowWatermark) {

        return lowWatermarkWriterExecutor.submit(new Callable<Void>() {
            @Override
            public Void call() throws IOException {
                try {
                    lwmWriteTimer.start();
                    lowWatermarkWriter.updateLowWatermark(lowWatermark);
                    lowWatermarkWriter.flush();
                } finally {
                    lwmWriteTimer.stop();
                }
                return null;
            }
        });

    }

    @Override
    public void close() throws IOException {

        LOG.info("Terminating Persistence Processor...");
        disruptor.halt();
        disruptor.shutdown();
        LOG.info("\tPersistence Processor Disruptor shutdown");
        disruptorExec.shutdownNow();
        try {
            disruptorExec.awaitTermination(3, SECONDS);
            LOG.info("\tPersistence Processor Disruptor executor shutdown");
        } catch (InterruptedException e) {
            LOG.error("Interrupted whilst finishing Persistence Processor Disruptor executor");
            Thread.currentThread().interrupt();
        }
        LOG.info("Persistence Processor terminated");

    }

    final static class PersistBatchEvent {

        private long batchSequence;
        private Batch batch;

        static void makePersistBatch(PersistBatchEvent e, long batchSequence, Batch batch) {
            e.batch = batch;
            e.batchSequence = batchSequence;
        }

        Batch getBatch() {
            return batch;
        }

        long getBatchSequence() {
            return batchSequence;
        }

        final static EventFactory<PersistBatchEvent> EVENT_FACTORY = new EventFactory<PersistBatchEvent>() {
            public PersistBatchEvent newInstance() {
                return new PersistBatchEvent();
            }
        };

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("batchSequence", batchSequence)
                    .add("batch", batch)
                    .toString();
        }

    }

}
