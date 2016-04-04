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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.yahoo.omid.metrics.Meter;
import com.yahoo.omid.metrics.MetricsRegistry;
import com.yahoo.omid.proto.TSOProto;

import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.codahale.metrics.MetricRegistry.name;

class ReplyProcessorImpl implements EventHandler<ReplyProcessorImpl.ReplyBatchEvent>, ReplyProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(ReplyProcessorImpl.class);

    final RingBuffer<ReplyBatchEvent> replyRing;
    final Meter abortMeter;
    final Meter commitMeter;
    final Meter timestampMeter;

    public static final int NO_ORDER = (-1);

    volatile private long nextIDToHandle;

    PriorityQueue<ReplyBatchEvent> futureEvents;

    @Inject
    ReplyProcessorImpl(MetricsRegistry metrics, Panicker panicker) {
        replyRing = RingBuffer.<ReplyBatchEvent>createMultiProducer(ReplyBatchEvent.EVENT_FACTORY, 1 << 12,
                                                               new BusySpinWaitStrategy());

        SequenceBarrier replySequenceBarrier = replyRing.newBarrier();
        BatchEventProcessor<ReplyBatchEvent> replyProcessor = new BatchEventProcessor<ReplyBatchEvent>(
            replyRing, replySequenceBarrier, this);
        replyProcessor.setExceptionHandler(new FatalExceptionHandler(panicker));

        replyRing.addGatingSequences(replyProcessor.getSequence());

        ExecutorService replyExec = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("reply-%d").build());
        replyExec.submit(replyProcessor);

        abortMeter = metrics.meter(name("tso", "aborts"));
        commitMeter = metrics.meter(name("tso", "commits"));
        timestampMeter = metrics.meter(name("tso", "timestampAllocation"));

        nextIDToHandle = 0L;
        futureEvents = new PriorityQueue<ReplyBatchEvent>(10, new Comparator<ReplyBatchEvent>() {
            public int compare(ReplyBatchEvent replyBatchEvent1, ReplyBatchEvent replyBatchEvent2) {
                return (Long.valueOf(replyBatchEvent1.getBatchID()).compareTo(replyBatchEvent2.getBatchID()));
            }});
    }

    public void reset() {
        nextIDToHandle = 0L;
    }

    private void handleReplyBatchEvent(ReplyBatchEvent event) {
        String name = null;
        Batch batch = event.getBatch();
        for (int i=0; i < batch.getNumEvents(); ++i) {
            PersistEvent localEvent = batch.events[i];

            switch (localEvent.getType()) {
            case COMMIT:
                name = "commitReplyProcessor";
                localEvent.getMonCtx().timerStart(name);
                handleCommitResponse(localEvent.getStartTimestamp(), localEvent.getCommitTimestamp(), localEvent.getChannel(), event.getMakeHeuristicDecision());
                localEvent.getMonCtx().timerStop(name);
                break;
            case ABORT:
                name = "abortReplyProcessor";
                localEvent.getMonCtx().timerStart(name);
                handleAbortResponse(localEvent.getStartTimestamp(), localEvent.getChannel());
                localEvent.getMonCtx().timerStop(name);
                break;
            case TIMESTAMP:
                name = "timestampReplyProcessor";
                localEvent.getMonCtx().timerStart(name);
                handleTimestampResponse(localEvent.getStartTimestamp(), localEvent.getChannel());
                localEvent.getMonCtx().timerStop(name);
                break;
            case LOW_WATERMARK:
                break;
            default:
                LOG.error("Unknown event {}", localEvent.getType());
                break;
            }
            localEvent.getMonCtx().publish();
        }
        batch.clear();
    }

    private void processWaitingEvents() {
        while (!futureEvents.isEmpty() && futureEvents.peek().getBatchID() == nextIDToHandle) {
            ReplyBatchEvent e = futureEvents.poll();
            handleReplyBatchEvent(e);
            ++nextIDToHandle;
        }
    }

    public void onEvent(ReplyBatchEvent event, long sequence, boolean endOfBatch) throws Exception {

        // Order of event's reply need to be guaranteed in order to preserve snapshot isolation.
        // This is done in order to present a scenario where a start id of N is returned
        // while commit smaller than still does not appear in the commit table.

        // If previous events were not processed yet (events contain smaller id)
        if (event.getBatchID() > nextIDToHandle) {
            futureEvents.add(event);
            return;
         }

        handleReplyBatchEvent(event);

        if (event.getBatchID() == NO_ORDER) {
            return;
        }

        ++ nextIDToHandle;

        // Process events that arrived before and kept in futureEvents.
        processWaitingEvents();
    }

    @Override
    public void batchResponse(Batch batch, long batchID, boolean makeHeuristicDecision) {
        long seq = replyRing.next();
        ReplyBatchEvent e = replyRing.get(seq);
        ReplyBatchEvent.makeReplyBatch(e, batch, batchID, makeHeuristicDecision);
        replyRing.publish(seq);
    }

    @Override
    public void addAbort(Batch batch, long startTimestamp, Channel c, MonitoringContext context) {
        batch.addAbort(startTimestamp, true, c, context);
        batchResponse(batch, NO_ORDER, false);
    }

    @Override
    public void addCommit(Batch batch, long startTimestamp, long commitTimestamp, Channel c, MonitoringContext context) {
        batch.addCommit(startTimestamp, commitTimestamp, c, context);
        batchResponse(batch, NO_ORDER, false);
    }

    void handleCommitResponse(long startTimestamp, long commitTimestamp, Channel c, boolean makeHeuristicDecision) {
        TSOProto.Response.Builder builder = TSOProto.Response.newBuilder();
        TSOProto.CommitResponse.Builder commitBuilder = TSOProto.CommitResponse.newBuilder();
        if (makeHeuristicDecision) { // If the commit is ambiguous is due to a new master TSO
            commitBuilder.setMakeHeuristicDecision(true);
        }
        commitBuilder.setAborted(false)
                .setStartTimestamp(startTimestamp)
                .setCommitTimestamp(commitTimestamp);
        builder.setCommitResponse(commitBuilder.build());
        c.write(builder.build());

        commitMeter.mark();
    }

    void handleAbortResponse(long startTimestamp, Channel c) {
        TSOProto.Response.Builder builder = TSOProto.Response.newBuilder();
        TSOProto.CommitResponse.Builder commitBuilder = TSOProto.CommitResponse.newBuilder();
        commitBuilder.setAborted(true)
                .setStartTimestamp(startTimestamp);
        builder.setCommitResponse(commitBuilder.build());
        c.write(builder.build());

        abortMeter.mark();
    }

    void handleTimestampResponse(long startTimestamp, Channel c) {
        TSOProto.Response.Builder builder = TSOProto.Response.newBuilder();
        TSOProto.TimestampResponse.Builder respBuilder = TSOProto.TimestampResponse.newBuilder();
        respBuilder.setStartTimestamp(startTimestamp);
        builder.setTimestampResponse(respBuilder.build());
        c.write(builder.build());

        timestampMeter.mark();
    }

    public final static class ReplyBatchEvent {
        Batch batch;
        long batchID;
        boolean makeHeuristicDecision;

        static void makeReplyBatch(ReplyBatchEvent e, Batch batch, long batchID, boolean makeHeuristicDecision) {
            e.batch = batch;
            e.batchID = batchID;
            e.makeHeuristicDecision = makeHeuristicDecision;
        }

        Batch getBatch() {
            return batch;
        }

        long getBatchID() {
            return batchID;
        }

        boolean getMakeHeuristicDecision() {
            return makeHeuristicDecision;
        }

        public final static EventFactory<ReplyBatchEvent> EVENT_FACTORY
        = new EventFactory<ReplyBatchEvent>() {
            public ReplyBatchEvent newInstance() {
                return new ReplyBatchEvent();
            }
        };
    }
}

