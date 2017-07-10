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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;

import org.apache.commons.pool2.ObjectPool;
import org.apache.omid.metrics.Meter;
import org.apache.omid.metrics.MetricsRegistry;
import org.apache.omid.proto.TSOProto;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static com.codahale.metrics.MetricRegistry.name;
import static com.lmax.disruptor.dsl.ProducerType.MULTI;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.omid.tso.ReplyProcessorImpl.ReplyBatchEvent.EVENT_FACTORY;

class ReplyProcessorImpl implements EventHandler<ReplyProcessorImpl.ReplyBatchEvent>, ReplyProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(ReplyProcessorImpl.class);

    // Disruptor-related attributes
    private final ExecutorService disruptorExec;
    private final Disruptor<ReplyBatchEvent> disruptor;
    private final RingBuffer<ReplyBatchEvent> replyRing;

    private final ObjectPool<Batch> batchPool;

    @VisibleForTesting
    AtomicLong nextIDToHandle = new AtomicLong();

    @VisibleForTesting
    PriorityQueue<ReplyBatchEvent> futureEvents;

    // Metrics
    private final Meter abortMeter;
    private final Meter commitMeter;
    private final Meter timestampMeter;
    private final Meter fenceMeter;

    @Inject
    ReplyProcessorImpl(@Named("ReplyStrategy") WaitStrategy strategy,
            MetricsRegistry metrics, Panicker panicker, ObjectPool<Batch> batchPool) {

        // ------------------------------------------------------------------------------------------------------------
        // Disruptor initialization
        // ------------------------------------------------------------------------------------------------------------

        ThreadFactoryBuilder threadFactory = new ThreadFactoryBuilder().setNameFormat("reply-%d");
        this.disruptorExec = Executors.newSingleThreadExecutor(threadFactory.build());

        this.disruptor = new Disruptor<>(EVENT_FACTORY, 1 << 12, disruptorExec, MULTI, strategy);
        disruptor.handleExceptionsWith(new FatalExceptionHandler(panicker));
        disruptor.handleEventsWith(this);
        this.replyRing = disruptor.start();

        // ------------------------------------------------------------------------------------------------------------
        // Attribute initialization
        // ------------------------------------------------------------------------------------------------------------

        this.batchPool = batchPool;
        this.nextIDToHandle.set(0);
        this.futureEvents = new PriorityQueue<>(10, new Comparator<ReplyBatchEvent>() {
            public int compare(ReplyBatchEvent replyBatchEvent1, ReplyBatchEvent replyBatchEvent2) {
                return Long.compare(replyBatchEvent1.getBatchSequence(), replyBatchEvent2.getBatchSequence());
            }
        });

        // Metrics config
        this.abortMeter = metrics.meter(name("tso", "aborts"));
        this.commitMeter = metrics.meter(name("tso", "commits"));
        this.timestampMeter = metrics.meter(name("tso", "timestampAllocation"));
        this.fenceMeter = metrics.meter(name("tso", "fences"));

        LOG.info("ReplyProcessor initialized");

    }

    @VisibleForTesting
    void handleReplyBatchEvent(ReplyBatchEvent replyBatchEvent) throws Exception {

        Batch batch = replyBatchEvent.getBatch();
        for (int i = 0; i < batch.getNumEvents(); i++) {
            PersistEvent event = batch.get(i);

            switch (event.getType()) {
                case COMMIT:
                    sendCommitResponse(event.getStartTimestamp(), event.getCommitTimestamp(), event.getChannel());
                    event.getMonCtx().timerStop("reply.processor.commit.latency");
                    commitMeter.mark();
                    break;
                case ABORT:
                    sendAbortResponse(event.getStartTimestamp(), event.getChannel());
                    event.getMonCtx().timerStop("reply.processor.abort.latency");
                    abortMeter.mark();
                    break;
                case TIMESTAMP:
                    sendTimestampResponse(event.getStartTimestamp(), event.getChannel());
                    event.getMonCtx().timerStop("reply.processor.timestamp.latency");
                    timestampMeter.mark();
                    break;
                case FENCE:
                    sendFenceResponse(event.getStartTimestamp(), event.getCommitTimestamp(), event.getChannel());
                    event.getMonCtx().timerStop("reply.processor.fence.latency");
                    fenceMeter.mark();
                    break;
                case COMMIT_RETRY:
                    throw new IllegalStateException("COMMIT_RETRY events must be filtered before this step: " + event);
                default:
                    throw new IllegalStateException("Event not allowed in Persistent Processor Handler: " + event);
            }
            event.getMonCtx().publish();
        }

        batchPool.returnObject(batch);

    }

    private void processWaitingEvents() throws Exception {

        while (!futureEvents.isEmpty() && futureEvents.peek().getBatchSequence() == nextIDToHandle.get()) {
            ReplyBatchEvent e = futureEvents.poll();
            handleReplyBatchEvent(e);
            nextIDToHandle.incrementAndGet();
        }

    }

    public void onEvent(ReplyBatchEvent event, long sequence, boolean endOfBatch) throws Exception {

        // Order of event's reply need to be guaranteed in order to preserve snapshot isolation.
        // This is done in order to present a scenario where a start id of N is returned
        // while commit smaller than still does not appear in the commit table.

        // If previous events were not processed yet (events contain smaller id)
        if (event.getBatchSequence() > nextIDToHandle.get()) {
            futureEvents.add(event);
            return;
        }

        handleReplyBatchEvent(event);

        nextIDToHandle.incrementAndGet();

        // Process events that arrived before and kept in futureEvents.
        processWaitingEvents();

    }

    @Override
    public void manageResponsesBatch(long batchSequence, Batch batch) {

        long seq = replyRing.next();
        ReplyBatchEvent e = replyRing.get(seq);
        ReplyBatchEvent.makeReplyBatch(e, batch, batchSequence);
        replyRing.publish(seq);

    }

    @Override
    public void sendCommitResponse(long startTimestamp, long commitTimestamp, Channel c) {

        TSOProto.Response.Builder builder = TSOProto.Response.newBuilder();
        TSOProto.CommitResponse.Builder commitBuilder = TSOProto.CommitResponse.newBuilder();
        commitBuilder.setAborted(false)
                .setStartTimestamp(startTimestamp)
                .setCommitTimestamp(commitTimestamp);
        builder.setCommitResponse(commitBuilder.build());
        c.write(builder.build());

    }

    @Override
    public void sendAbortResponse(long startTimestamp, Channel c) {

        TSOProto.Response.Builder builder = TSOProto.Response.newBuilder();
        TSOProto.CommitResponse.Builder commitBuilder = TSOProto.CommitResponse.newBuilder();
        commitBuilder.setAborted(true);
        commitBuilder.setStartTimestamp(startTimestamp);
        builder.setCommitResponse(commitBuilder.build());
        c.write(builder.build());

    }

    @Override
    public void sendTimestampResponse(long startTimestamp, Channel c) {

        TSOProto.Response.Builder builder = TSOProto.Response.newBuilder();
        TSOProto.TimestampResponse.Builder respBuilder = TSOProto.TimestampResponse.newBuilder();
        respBuilder.setStartTimestamp(startTimestamp);
        builder.setTimestampResponse(respBuilder.build());
        c.write(builder.build());

    }

    @Override
    public void sendFenceResponse(long tableID, long fenceTimestamp, Channel c) {

        TSOProto.Response.Builder builder = TSOProto.Response.newBuilder();
        TSOProto.FenceResponse.Builder fenceBuilder = TSOProto.FenceResponse.newBuilder();
        fenceBuilder.setTableId(tableID);
        fenceBuilder.setFenceId(fenceTimestamp);
        builder.setFenceResponse(fenceBuilder.build());
        c.write(builder.build());

    }

    @Override
    public void close() {

        LOG.info("Terminating Reply Processor...");
        disruptor.halt();
        disruptor.shutdown();
        LOG.info("\tReply Processor Disruptor shutdown");
        disruptorExec.shutdownNow();
        try {
            disruptorExec.awaitTermination(3, SECONDS);
            LOG.info("\tReply Processor Disruptor executor shutdown");
        } catch (InterruptedException e) {
            LOG.error("Interrupted whilst finishing Reply Processor Disruptor executor");
            Thread.currentThread().interrupt();
        }
        LOG.info("Reply Processor terminated");

    }

    final static class ReplyBatchEvent {

        private Batch batch;
        private long batchSequence;

        static void makeReplyBatch(ReplyBatchEvent e, Batch batch, long batchSequence) {
            e.batch = batch;
            e.batchSequence = batchSequence;
        }

        Batch getBatch() {
            return batch;
        }

        long getBatchSequence() {
            return batchSequence;
        }

        final static EventFactory<ReplyBatchEvent> EVENT_FACTORY = new EventFactory<ReplyBatchEvent>() {
            public ReplyBatchEvent newInstance() {
                return new ReplyBatchEvent();
            }
        };

    }

}

