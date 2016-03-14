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
package com.yahoo.omid.tso;

import static com.codahale.metrics.MetricRegistry.name;
import static com.yahoo.omid.tso.TSOServer.TSO_HOST_AND_PORT_KEY;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.inject.Inject;
import javax.inject.Named;

import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.WorkerPool;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.metrics.Histogram;
import com.yahoo.omid.metrics.Meter;
import com.yahoo.omid.metrics.MetricsRegistry;
import com.yahoo.omid.metrics.Timer;

import com.yahoo.omid.tso.PersistenceProcessorImpl.PersistEvent.Type;
import com.yahoo.omid.tso.PersistenceProcessorImpl.PersistenceProcessorHandler.Batch;
import com.yahoo.omid.tso.PersistenceProcessorImpl.PersistenceProcessorHandler.BatchPool;

class PersistenceProcessorImpl
    implements PersistenceProcessor {

    static final int DEFAULT_PERSIST_HANDLER_NUM = 1;
    static final int DEFAULT_MAX_BATCH_SIZE = 10000 / DEFAULT_PERSIST_HANDLER_NUM;
    static final String TSO_MAX_BATCH_SIZE_KEY = "tso.maxbatchsize";
    static final int DEFAULT_BATCH_PERSIST_TIMEOUT_MS = 100;
    static final String TSO_BATCH_PERSIST_TIMEOUT_MS_KEY = "tso.batch-persist-timeout-ms";
    static final String TSO_PERSIST_HANDLER_NUM = "tso.persist.handler";
    static final int NUM_BUFFERS_PER_HANDLER = 10;

    final ReplyProcessor reply;
    final RetryProcessor retryProc;
    final CommitTable.Client commitTableClient;
    final CommitTable.Writer writer;
    final Panicker panicker;
    final RingBuffer<PersistBatchEvent> persistRing;

    private PersistenceProcessorHandler[] handlers;
    private int numHandlers;

    private BatchPool batchPool;
    private Batch batch;
    private long batchIDCnt;

    private long lowWatermark;
    MonitoringContext lowWatermarkContext;

    long lastFlush = System.nanoTime();
    final static int BATCH_TIMEOUT_MS = 100;

    final Meter timeoutMeter;

    @Inject
    PersistenceProcessorImpl(MetricsRegistry metrics,
                             @Named(TSO_HOST_AND_PORT_KEY) String tsoHostAndPort,
                             LeaseManagement leaseManager,
                             CommitTable commitTable,
                             ReplyProcessor reply,
                             RetryProcessor retryProc,
                             Panicker panicker,
                             TSOServerCommandLineConfig config)
            throws IOException {
        this(metrics,
                null,
                tsoHostAndPort,
                leaseManager,
                commitTable,
                reply,
                retryProc,
                panicker,
                config);
    }

    @VisibleForTesting
    PersistenceProcessorImpl(MetricsRegistry metrics,
                             Batch batch,
                             String tsoHostAndPort,
                             LeaseManagement leaseManager,
                             CommitTable commitTable,
                             ReplyProcessor reply,
                             RetryProcessor retryProc,
                             Panicker panicker,
                             TSOServerCommandLineConfig config)
            throws IOException {

        this.batch = batch;
        this.commitTableClient = commitTable.getClient();
        this.writer = commitTable.getWriter();
        this.reply = reply;
        this.retryProc = retryProc;
        this.panicker = panicker;

        batchIDCnt = 0L;

        batchPool = new BatchPool(config, batch);

        this.batch = batchPool.getNextEmptyBatch();

        timeoutMeter = metrics.meter(name("tso", "persist", "timeout"));

        persistRing = RingBuffer.<PersistBatchEvent>createSingleProducer(
                PersistBatchEvent.EVENT_FACTORY, 1 << 20, new BusySpinWaitStrategy());
        SequenceBarrier persistSequenceBarrier = persistRing.newBarrier();

        numHandlers = config.getPersistHandlerNum();
        handlers = new PersistenceProcessorHandler[numHandlers];
        for (int i = 0; i < config.getPersistHandlerNum(); i++) {
            handlers[i] = new PersistenceProcessorHandler(metrics, tsoHostAndPort, leaseManager, commitTable, reply, retryProc, panicker, config);
        }

        WorkerPool<PersistBatchEvent> requestProcessor = new WorkerPool<PersistBatchEvent>(persistRing, persistSequenceBarrier,
                                                            new FatalExceptionHandler(panicker), handlers);
        persistRing.addGatingSequences(requestProcessor.getWorkerSequences());

        ExecutorService requestExec = Executors.newFixedThreadPool(
                config.getPersistHandlerNum(), new ThreadFactoryBuilder().setNameFormat("persist-%d").build());

        requestProcessor.start(requestExec);
    }

    @Override
    public void reset() {
        batchIDCnt = 0L;

        batchPool.reset();
        batch = batchPool.getNextEmptyBatch();

        reply.reset();
    }

    @VisibleForTesting
    void setBatch(Batch batch) {
        this.batch = batch;
    }

    @Override
    public void persistFlush(boolean forTesting) {
        if (!forTesting && batch.getNumEvents() == 0) {
            return;
        }
        lastFlush = System.nanoTime();
        batch.addLowWatermark(this.lowWatermark, this.lowWatermarkContext);
        long seq = persistRing.next();
        PersistBatchEvent e = persistRing.get(seq);
        PersistBatchEvent.makePersistBatch(e, batch, batchIDCnt++);
        persistRing.publish(seq);

        batch = batchPool.getNextEmptyBatch();
    }

    @Override
    public void persistCommit(long startTimestamp, long commitTimestamp, Channel c, MonitoringContext monCtx)  {
        batch.addCommit(startTimestamp, commitTimestamp, c, monCtx);
        if (batch.isLastEmptyEntry()) {
            persistFlush(false);
        }
    }

    @Override
    public void persistAbort(long startTimestamp, boolean isRetry, Channel c, MonitoringContext context)  {
        batch.addAbort(startTimestamp, isRetry, c, context);
        if (batch.isLastEmptyEntry()) {
            persistFlush(false);
        }
    }

    @Override
    public void persistTimestamp(long startTimestamp, Channel c, MonitoringContext context)  {
        batch.addTimestamp(startTimestamp, c, context);
        if (batch.isLastEmptyEntry()) {
            persistFlush(false);
        }
    }

    @Override
    public void persistLowWatermark(long lowWatermark, MonitoringContext context) {
        this.lowWatermark = lowWatermark;
        this.lowWatermarkContext = context;
    }

    public final static class PersistBatchEvent {
        Batch batch;
        long batchID;

        static void makePersistBatch(PersistBatchEvent e, Batch batch, long batchID) {
            e.batch = batch;
            e.batchID = batchID;
        }

        Batch getBatch() {
            return batch;
        }

        long getBatchID() {
            return batchID;
        }

        public final static EventFactory<PersistBatchEvent> EVENT_FACTORY
        = new EventFactory<PersistBatchEvent>() {
            public PersistBatchEvent newInstance() {
                return new PersistBatchEvent();
            }
        };
    }

    public final static class PersistEvent {

        private MonitoringContext monCtx;

        enum Type {
            TIMESTAMP, COMMIT, ABORT, LOW_WATERMARK
        }
        private Type type = null;
        private Channel channel = null;

        private boolean isRetry = false;
        private long startTimestamp = 0L;
        private long commitTimestamp = 0L;
        private long lowWatermark = 0L;

        static void makePersistCommit(PersistEvent e, long startTimestamp, long commitTimestamp, Channel c,
                                      MonitoringContext monCtx) {
            e.type = Type.COMMIT;
            e.startTimestamp = startTimestamp;
            e.commitTimestamp = commitTimestamp;
            e.channel = c;
            e.monCtx = monCtx;
        }

        static void makePersistAbort(PersistEvent e, long startTimestamp, boolean isRetry, Channel c,
                                     MonitoringContext monCtx) {
            e.type = Type.ABORT;
            e.startTimestamp = startTimestamp;
            e.isRetry = isRetry;
            e.channel = c;
            e.monCtx = monCtx;
        }

        static void makePersistTimestamp(PersistEvent e, long startTimestamp, Channel c, MonitoringContext monCtx) {
            e.type = Type.TIMESTAMP;
            e.startTimestamp = startTimestamp;
            e.channel = c;
            e.monCtx = monCtx;
        }

        static void makePersistLowWatermark(PersistEvent e, long lowWatermark, MonitoringContext monCtx) {
            e.type = Type.LOW_WATERMARK;
            e.lowWatermark = lowWatermark;
            e.monCtx = monCtx;
        }

        MonitoringContext getMonCtx() {
            return monCtx;
        }

        Type getType() {
            return type;
        }

        Channel getChannel() {
            return channel;
        }

        boolean isRetry() {
            return isRetry;
        }

        long getStartTimestamp() {
            return startTimestamp;
        }

        long getCommitTimestamp() {
            return commitTimestamp;
        }

        long getLowWatermark() {
            return lowWatermark;
        }
    }

    static class PersistenceProcessorHandler
    implements
    WorkHandler<PersistenceProcessorImpl.PersistBatchEvent> {

        private static final Logger LOG = LoggerFactory.getLogger(PersistenceProcessor.class);

        private final String tsoHostAndPort;
        private final LeaseManagement leaseManager;

        final ReplyProcessor reply;
        final RetryProcessor retryProc;
        final CommitTable.Client commitTableClient;
        final CommitTable.Writer writer;
        final Panicker panicker;

        final int maxBatchSize;

        Batch handlerBatch;

        final Timer flushTimer;
        final Histogram batchSizeHistogram;

        long lastFlush;

        PersistenceProcessorHandler(MetricsRegistry metrics,
                String tsoHostAndPort,
                LeaseManagement leaseManager,
                CommitTable commitTable,
                ReplyProcessor reply,
                RetryProcessor retryProc,
                Panicker panicker,
                TSOServerCommandLineConfig config)
                        throws IOException {

            this.tsoHostAndPort = tsoHostAndPort;
            this.leaseManager = leaseManager;
            this.commitTableClient = commitTable.getClient();
            this.writer = commitTable.getWriter();
            this.reply = reply;
            this.retryProc = retryProc;
            this.panicker = panicker;
            this.maxBatchSize = config.getMaxBatchSize();

            LOG.info("Creating the persist processor with batch size {}, and timeout {}ms",
                    maxBatchSize, config.getBatchPersistTimeoutMS());

            flushTimer = metrics.timer(name("tso", "persist", "flush"));
            batchSizeHistogram = metrics.histogram(name("tso", "persist", "batchsize"));
        }

        @Override
        public void onEvent(PersistenceProcessorImpl.PersistBatchEvent event) throws Exception {
            Batch batch = event.getBatch();
            handlerBatch = batch;
            for (int i=0; i < batch.getNumEvents(); ++i) {
                PersistenceProcessorImpl.PersistEvent localEvent = batch.events[i];

                switch (localEvent.getType()) {
                case COMMIT:
                    localEvent.getMonCtx().timerStart("commitPersistProcessor");
                    // TODO: What happens when the IOException is thrown?
                    writer.addCommittedTransaction(localEvent.getStartTimestamp(), localEvent.getCommitTimestamp());
                    break;
                case ABORT:
                    break;
                case TIMESTAMP:
                    localEvent.getMonCtx().timerStart("timestampPersistProcessor");
                    break;
                case LOW_WATERMARK:
                    writer.updateLowWatermark(localEvent.getLowWatermark());
                    break;
                default:
                    assert(false);
                }
            }
            flush(event.getBatchID());
        }

        private void flush(long batchID) throws IOException {
            lastFlush = System.nanoTime();

            boolean areWeStillMaster = true;
            if (!leaseManager.stillInLeasePeriod()) {
                // The master TSO replica has changed, so we must inform the
                // clients about it when sending the replies and avoid flushing
                // the current batch of TXs
                areWeStillMaster = false;
                // We need also to clear the data in the buffer
                writer.clearWriteBuffer();
                LOG.trace("Replica {} lost mastership before flushig data", tsoHostAndPort);
            } else {
                try {
                    writer.flush();
                } catch (IOException e) {
                    panicker.panic("Error persisting commit batch", e.getCause());
                }
                batchSizeHistogram.update(handlerBatch.getNumEvents());
                if (!leaseManager.stillInLeasePeriod()) {
                    // If after flushing this TSO server is not the master
                    // replica we need inform the client about it
                    areWeStillMaster = false;
                    LOG.warn("Replica {} lost mastership after flushig data", tsoHostAndPort);
                }
            }
            flushTimer.update((System.nanoTime() - lastFlush));
            handlerBatch.sendReply(reply, retryProc, batchID, areWeStillMaster);
        }

        public static class BatchPool {
            private Batch batchForTesting;
            private Batch[] batches;
            private int emptyBatch;
            private int poolSize;

            public BatchPool(TSOServerCommandLineConfig config, Batch batchForTesting) {
                emptyBatch = 0;
                poolSize = config.getPersistHandlerNum() * config.getNumBuffersPerHandler();

                if (batchForTesting != null) {
                    this.batchForTesting = batchForTesting;
                } else {
                    batches = new Batch[poolSize];
                    for (int i=0; i < poolSize; ++i) {
                        batches[i] = new Batch(config.getMaxBatchSize(), i);
                    }
                }
            }

            public Batch getNextEmptyBatch() {
                if (batchForTesting != null) {
                    return batchForTesting;
                } else {
                    for (;batches[emptyBatch].getNumEvents() != 0; emptyBatch = (emptyBatch + 1) % (poolSize));
                    return batches[emptyBatch];
                }
            }

            public void reset() {
                for (int i=0; i < poolSize; ++i) {
                    batches[i].clear();
                }
                emptyBatch = 0;
            }
        }

        public static class Batch {
            final PersistEvent[] events;
            final int maxBatchSize;
            int numEvents;
            int id;

            Batch(int maxBatchSize, int id) {
                assert (maxBatchSize > 0);
                this.maxBatchSize = maxBatchSize;
                events = new PersistEvent[maxBatchSize];
                numEvents = 0;
                for (int i = 0; i < maxBatchSize; i++) {
                    events[i] = new PersistEvent();
                }
                this.id = id;
            }

            int getID() {
                return id;
            }

            boolean isFull() {
                assert (numEvents <= maxBatchSize);
                return numEvents == maxBatchSize;
            }

            boolean isLastEmptyEntry() {
                assert (numEvents <= maxBatchSize);
                return numEvents == (maxBatchSize - 1);
            }

            int getNumEvents() {
                return numEvents;
            }

            void clear() {
                numEvents = 0;
            }

            void addCommit(long startTimestamp, long commitTimestamp, Channel c, MonitoringContext context) {
                if (isFull()) {
                    throw new IllegalStateException("batch full");
                }
                int index = numEvents++;
                PersistEvent e = events[index];
                PersistEvent.makePersistCommit(e, startTimestamp, commitTimestamp, c, context);
            }

            void addAbort(long startTimestamp, boolean isRetry, Channel c, MonitoringContext context) {
                if (isFull()) {
                    throw new IllegalStateException("batch full");
                }
                int index = numEvents++;
                PersistEvent e = events[index];
                PersistEvent.makePersistAbort(e, startTimestamp, isRetry, c, context);
            }

            void addUndecidedRetriedRequest(long startTimestamp, Channel c, MonitoringContext context) {
                if (isFull()) {
                    throw new IllegalStateException("batch full");
                }
                int index = numEvents++;
                PersistEvent e = events[index];
                // We mark the event as an ABORT retry to identify the events to send
                // to the retry processor
                PersistEvent.makePersistAbort(e, startTimestamp, true, c, context);
            }

            void addTimestamp(long startTimestamp, Channel c, MonitoringContext context) {
                if (isFull()) {
                    throw new IllegalStateException("batch full");
                }
                int index = numEvents++;
                PersistEvent e = events[index];
                PersistEvent.makePersistTimestamp(e, startTimestamp, c, context);
            }

            void addLowWatermark(long lowWatermark, MonitoringContext context) {
                if (isFull()) {
                    throw new IllegalStateException("batch full");
                }
                int index = numEvents++;
                PersistEvent e = events[index];
                PersistEvent.makePersistLowWatermark(e, lowWatermark, context);
            }

            void sendReply(ReplyProcessor reply, RetryProcessor retryProc, long batchID, boolean isTSOInstanceMaster) {
                for (int i = 0; i < numEvents;) {
                    PersistEvent e = events[i];
                    if (e.getType() == Type.ABORT && e.isRetry()) {
                        retryProc.disambiguateRetryRequestHeuristically(e.getStartTimestamp(), e.getChannel(), e.getMonCtx());
                        events[i] = events[numEvents - 1];
                        if (numEvents == 1) {
                            numEvents = 0;
                            break;
                        }
                        --numEvents;
                        continue;
                    }
                    ++i;
                }

                reply.batchResponse(this, batchID, !isTSOInstanceMaster);
            }
        }
    }
}
