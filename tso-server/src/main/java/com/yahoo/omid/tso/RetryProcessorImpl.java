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

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.CommitTable.CommitTimestamp;
import com.yahoo.omid.metrics.Meter;
import com.yahoo.omid.metrics.MetricsRegistry;
import org.jboss.netty.channel.Channel;
import com.yahoo.omid.tso.PersistenceProcessorImpl.PersistenceProcessorHandler.Batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Manages the retry requests that clients can send when they did
 * not received the response in the specified timeout
 */
class RetryProcessorImpl implements EventHandler<RetryProcessorImpl.RetryEvent>, RetryProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(RetryProcessor.class);

    // Disruptor chain stuff
    final ReplyProcessor replyProc;
    final RingBuffer<RetryEvent> retryRing;

    final CommitTable.Client commitTableClient;
    final CommitTable.Writer writer;

    // Metrics
    final Meter retriesMeter;

    @Inject
    RetryProcessorImpl(MetricsRegistry metrics, CommitTable commitTable, ReplyProcessor replyProc, Panicker panicker)
            throws IOException
    {

        this.commitTableClient = commitTable.getClient();
        this.writer = commitTable.getWriter();
        this.replyProc = replyProc;

        WaitStrategy strategy = new YieldingWaitStrategy();

        retryRing = RingBuffer.createSingleProducer(RetryEvent.EVENT_FACTORY, 1 << 12, strategy);
        SequenceBarrier retrySeqBarrier = retryRing.newBarrier();
        BatchEventProcessor<RetryEvent> retryProcessor = new BatchEventProcessor<>(retryRing, retrySeqBarrier, this);
        retryProcessor.setExceptionHandler(new FatalExceptionHandler(panicker));

        retryRing.addGatingSequences(retryProcessor.getSequence());

        ExecutorService retryExec = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("retry-%d").build());
        retryExec.submit(retryProcessor);

        // Metrics
        retriesMeter = metrics.meter(name("tso", "retries"));
    }

    @Override
    public void onEvent(final RetryEvent event, final long sequence, final boolean endOfBatch)
            throws Exception {

        switch (event.getType()) {
            case COMMIT:
                // TODO: What happens when the IOException is thrown?
                handleCommitRetry(event);
                break;
            default:
                assert (false);
                break;
        }

    }

    public void addAbort(Batch batch, long startTimestamp, Channel c, MonitoringContext context) {
        batch.addAbort(startTimestamp, true, c, context);
    }

    public void addCommit(Batch batch, long startTimestamp, long commitTimestamp, Channel c, MonitoringContext context) {
        batch.addCommit(startTimestamp, commitTimestamp, c, context);
    }

    private void handleCommitRetry(RetryEvent event) throws InterruptedException, ExecutionException {
        long startTimestamp = event.getStartTimestamp();
        try {
            Optional<CommitTimestamp> commitTimestamp = commitTableClient.getCommitTimestamp(startTimestamp).get();
            Batch batch = new Batch(1, -1);
            if(commitTimestamp.isPresent()) {
                if (commitTimestamp.get().isValid()) {
                    LOG.trace("Valid commit TS found in Commit Table");
                    replyProc.addCommit(batch, startTimestamp, commitTimestamp.get().getValue(), event.getChannel(), event.getMonCtx());
                } else {
                    LOG.trace("Invalid commit TS found in Commit Table");
                    replyProc.addAbort(batch, startTimestamp, event.getChannel(), event.getMonCtx());
                }
            } else {
                LOG.trace("No commit TS found in Commit Table");
                replyProc.addAbort(batch, startTimestamp, event.getChannel(), event.getMonCtx());
            }
        } catch (InterruptedException e) {
            LOG.error("Interrupted reading from commit table");
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            LOG.error("Error reading from commit table", e);
        }

        retriesMeter.mark();
    }

    @Override
    public void disambiguateRetryRequestHeuristically(long startTimestamp, Channel c, MonitoringContext monCtx) {
        long seq = retryRing.next();
        RetryEvent e = retryRing.get(seq);
        RetryEvent.makeCommitRetry(e, startTimestamp, c, monCtx);
        retryRing.publish(seq);
    }

    public final static class RetryEvent {

        enum Type {
            COMMIT
        }

        private Type type = null;

        private long startTimestamp = 0;
        private Channel channel = null;
        private MonitoringContext monCtx;

        static void makeCommitRetry(RetryEvent e, long startTimestamp, Channel c, MonitoringContext monCtx) {
            e.monCtx = monCtx;
            e.type = Type.COMMIT;
            e.startTimestamp = startTimestamp;
            e.channel = c;
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

        long getStartTimestamp() {
            return startTimestamp;
        }

        public final static EventFactory<RetryEvent> EVENT_FACTORY
                = new EventFactory<RetryEvent>() {
            @Override
            public RetryEvent newInstance() {
                return new RetryEvent();
            }
        };
    }

}
