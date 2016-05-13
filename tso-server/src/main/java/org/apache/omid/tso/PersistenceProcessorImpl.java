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
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WorkerPool;
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

import static org.apache.omid.metrics.MetricsUtils.name;
import static org.apache.omid.tso.PersistenceProcessorImpl.PersistBatchEvent.EVENT_FACTORY;
import static org.apache.omid.tso.PersistenceProcessorImpl.PersistBatchEvent.makePersistBatch;

class PersistenceProcessorImpl implements PersistenceProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(PersistenceProcessorImpl.class);

    private static final long INITIAL_LWM_VALUE = -1L;

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
                             CommitTable commitTable,
                             ObjectPool<Batch> batchPool,
                             Panicker panicker,
                             PersistenceProcessorHandler[] handlers,
                             MetricsRegistry metrics)
            throws Exception {

        this.metrics = metrics;
        this.lowWatermarkWriter = commitTable.getWriter();
        this.batchSequence = 0L;
        this.batchPool = batchPool;
        this.currentBatch = batchPool.borrowObject();

        // Low Watermark writer
        ThreadFactoryBuilder lwmThreadFactory = new ThreadFactoryBuilder().setNameFormat("lwm-writer-%d");
        lowWatermarkWriterExecutor = Executors.newSingleThreadExecutor(lwmThreadFactory.build());

        // Disruptor configuration
        this.persistRing = RingBuffer.createSingleProducer(EVENT_FACTORY, 1 << 20, new BusySpinWaitStrategy());

        ThreadFactoryBuilder threadFactory = new ThreadFactoryBuilder().setNameFormat("persist-%d");
        ExecutorService requestExec = Executors.newFixedThreadPool(config.getNumConcurrentCTWriters(),
                                                                   threadFactory.build());

        WorkerPool<PersistBatchEvent> persistProcessor = new WorkerPool<>(persistRing,
                                                                          persistRing.newBarrier(),
                                                                          new FatalExceptionHandler(panicker),
                                                                          handlers);
        this.persistRing.addGatingSequences(persistProcessor.getWorkerSequences());
        persistProcessor.start(requestExec);

        // Metrics config
        this.lwmWriteTimer = metrics.timer(name("tso", "lwmWriter", "latency"));

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
    public void addAbortToBatch(long startTimestamp, boolean isCommitRetry, Channel c, MonitoringContext context)
            throws Exception {

        currentBatch.addAbort(startTimestamp, isCommitRetry, c, context);
        if (currentBatch.isFull()) {
            triggerCurrentBatchFlush();
        }

    }

    @Override
    public void addTimestampToBatch(long startTimestamp, Channel c, MonitoringContext context) throws Exception {

        currentBatch.addTimestamp(startTimestamp, c, context);
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
