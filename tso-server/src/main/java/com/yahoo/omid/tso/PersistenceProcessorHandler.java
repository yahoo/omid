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

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lmax.disruptor.WorkHandler;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.metrics.Histogram;
import com.yahoo.omid.metrics.MetricsRegistry;
import com.yahoo.omid.metrics.Timer;
import com.yahoo.omid.tso.BatchPool.Batch;
import com.yahoo.omid.tso.PersistEvent.Type;

    public class PersistenceProcessorHandler
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
                TSOServerConfig config)
                        throws InterruptedException, ExecutionException, IOException {

            this.tsoHostAndPort = tsoHostAndPort;
            this.leaseManager = leaseManager;
            this.commitTableClient = commitTable.getClient();
            this.writer = commitTable.getWriter();
            this.reply = reply;
            this.retryProc = retryProc;
            this.panicker = panicker;
            this.maxBatchSize = config.getMaxBatchSize() / config.getPersistHandlerNum();

            LOG.info("Creating the persist processor handler with batch size {}",
                    maxBatchSize);

            flushTimer = metrics.timer(name("tso", "persist", "flush"));
            batchSizeHistogram = metrics.histogram(name("tso", "persist", "batchsize"));
        }

        @Override
        public void onEvent(PersistenceProcessorImpl.PersistBatchEvent event) throws Exception {
            Batch batch = event.getBatch();
            handlerBatch = batch;
            for (int i=0; i < batch.getNumEvents(); ++i) {
                PersistEvent localEvent = batch.events[i];

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

//        public static class BatchPool {
//            final private Batch[] batches;
//            final private int poolSize;
//            private int emptyBatch;
//            
//            public BatchPool(TSOServerConfig config) {
//                poolSize = config.getPersistHandlerNum() * config.getNumBuffersPerHandler();
//                batches = new Batch[poolSize];
//                for (int i=0; i < poolSize; ++i) {
//                    batches[i] = new Batch(config.getMaxBatchSize() / config.getPersistHandlerNum());
//                }
//                emptyBatch = 0;
//            }
//
//            public Batch getNextEmptyBatch() {
//                for (;batches[emptyBatch].getNumEvents() != 0; emptyBatch = (emptyBatch + 1) % (poolSize));
//                return batches[emptyBatch];
//            }
//
//            public void reset() {
//                for (int i=0; i < poolSize; ++i) {
//                    batches[i].clear();
//                }
//                emptyBatch = 0;
//            }
//        }
//
//        public static class Batch {
//            final PersistEvent[] events;
//            final int maxBatchSize;
//            int numEvents;
//
//
//            Batch(int maxBatchSize) {
//                assert (maxBatchSize > 0);
//                this.maxBatchSize = maxBatchSize;
//                events = new PersistEvent[maxBatchSize];
//                numEvents = 0;
//                for (int i = 0; i < maxBatchSize; i++) {
//                    events[i] = new PersistEvent();
//                }
//            }
//
//            boolean isFull() {
//                assert (numEvents <= maxBatchSize);
//                return numEvents == maxBatchSize;
//            }
//
//            boolean isEmpty() {
//                return numEvents == 0;
//            }
//
//            boolean isLastEmptyEntry() {
//                assert (numEvents <= maxBatchSize);
//                return numEvents == (maxBatchSize - 1);
//            }
//
//            int getNumEvents() {
//                return numEvents;
//            }
//
//            void clear() {
//                numEvents = 0;
//            }
//
//            void addCommit(long startTimestamp, long commitTimestamp, Channel c, MonitoringContext context) {
//                if (isFull()) {
//                    throw new IllegalStateException("batch full");
//                }
//                int index = numEvents++;
//                PersistEvent e = events[index];
//                e.makePersistCommit(startTimestamp, commitTimestamp, c, context);
//            }
//
//            void addAbort(long startTimestamp, boolean isRetry, Channel c, MonitoringContext context) {
//                if (isFull()) {
//                    throw new IllegalStateException("batch full");
//                }
//                int index = numEvents++;
//                PersistEvent e = events[index];
//                e.makePersistAbort(startTimestamp, isRetry, c, context);
//            }
//
//            void addUndecidedRetriedRequest(long startTimestamp, Channel c, MonitoringContext context) {
//                if (isFull()) {
//                    throw new IllegalStateException("batch full");
//                }
//                int index = numEvents++;
//                PersistEvent e = events[index];
//                // We mark the event as an ABORT retry to identify the events to send
//                // to the retry processor
//                e.makePersistAbort(startTimestamp, true, c, context);
//            }
//
//            void addTimestamp(long startTimestamp, Channel c, MonitoringContext context) {
//                if (isFull()) {
//                    throw new IllegalStateException("batch full");
//                }
//                int index = numEvents++;
//                PersistEvent e = events[index];
//                e.makePersistTimestamp(startTimestamp, c, context);
//            }
//
//            void addLowWatermark(long lowWatermark, MonitoringContext context) {
//                if (isFull()) {
//                    throw new IllegalStateException("batch full");
//                }
//                int index = numEvents++;
//                PersistEvent e = events[index];
//                e.makePersistLowWatermark(lowWatermark, context);
//            }
//
//            void sendReply(ReplyProcessor reply, RetryProcessor retryProc, long batchID, boolean isTSOInstanceMaster) {
//                for (int i = 0; i < numEvents;) {
//                    PersistEvent e = events[i];
//                    if (e.getType() == Type.ABORT && e.isRetry()) {
//                        retryProc.disambiguateRetryRequestHeuristically(e.getStartTimestamp(), e.getChannel(), e.getMonCtx());
//                        events[i] = events[numEvents - 1];
//                        if (numEvents == 1) {
//                            numEvents = 0;
//                            break;
//                        }
//                        --numEvents;
//                        continue;
//                    }
//                    ++i;
//                }
//
//                reply.batchResponse(this, batchID, !isTSOInstanceMaster);
//            }
//        }
    }
