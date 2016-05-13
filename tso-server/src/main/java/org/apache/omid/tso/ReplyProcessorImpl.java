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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
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
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import static com.codahale.metrics.MetricRegistry.name;

class ReplyProcessorImpl implements EventHandler<ReplyProcessorImpl.ReplyBatchEvent>, ReplyProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(ReplyProcessorImpl.class);

    private final ObjectPool<Batch> batchPool;

    private final RingBuffer<ReplyBatchEvent> replyRing;

    private AtomicLong nextIDToHandle = new AtomicLong();

    private PriorityQueue<ReplyBatchEvent> futureEvents;

    // Metrics
    private final Meter abortMeter;
    private final Meter commitMeter;
    private final Meter timestampMeter;

    @Inject
    ReplyProcessorImpl(MetricsRegistry metrics, Panicker panicker, ObjectPool<Batch> batchPool) {

        this.batchPool = batchPool;

        this.nextIDToHandle.set(0);

        this.replyRing = RingBuffer.createMultiProducer(ReplyBatchEvent.EVENT_FACTORY, 1 << 12, new BusySpinWaitStrategy());

        SequenceBarrier replySequenceBarrier = replyRing.newBarrier();
        BatchEventProcessor<ReplyBatchEvent> replyProcessor = new BatchEventProcessor<>(replyRing, replySequenceBarrier, this);
        replyProcessor.setExceptionHandler(new FatalExceptionHandler(panicker));

        replyRing.addGatingSequences(replyProcessor.getSequence());

        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("reply-%d").build();
        ExecutorService replyExec = Executors.newSingleThreadExecutor(threadFactory);
        replyExec.submit(replyProcessor);

        this.futureEvents = new PriorityQueue<>(10, new Comparator<ReplyBatchEvent>() {
            public int compare(ReplyBatchEvent replyBatchEvent1, ReplyBatchEvent replyBatchEvent2) {
                return Long.compare(replyBatchEvent1.getBatchSequence(), replyBatchEvent2.getBatchSequence());
            }
        });

        this.abortMeter = metrics.meter(name("tso", "aborts"));
        this.commitMeter = metrics.meter(name("tso", "commits"));
        this.timestampMeter = metrics.meter(name("tso", "timestampAllocation"));

    }

    private void handleReplyBatchEvent(ReplyBatchEvent replyBatchEvent) throws Exception {

        String name;
        Batch batch = replyBatchEvent.getBatch();
        for (int i = 0; i < batch.getNumEvents(); i++) {
            PersistEvent event = batch.get(i);

            switch (event.getType()) {
            case COMMIT:
                name = "commitReplyProcessor";
                event.getMonCtx().timerStart(name);
                sendCommitResponse(event.getStartTimestamp(), event.getCommitTimestamp(), event.getChannel());
                event.getMonCtx().timerStop(name);
                break;
            case ABORT:
                name = "abortReplyProcessor";
                event.getMonCtx().timerStart(name);
                sendAbortResponse(event.getStartTimestamp(), event.getChannel());
                event.getMonCtx().timerStop(name);
                break;
            case TIMESTAMP:
                name = "timestampReplyProcessor";
                event.getMonCtx().timerStart(name);
                sendTimestampResponse(event.getStartTimestamp(), event.getChannel());
                event.getMonCtx().timerStop(name);
                break;
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

        commitMeter.mark();

    }

    @Override
    public void sendAbortResponse(long startTimestamp, Channel c) {

        TSOProto.Response.Builder builder = TSOProto.Response.newBuilder();
        TSOProto.CommitResponse.Builder commitBuilder = TSOProto.CommitResponse.newBuilder();
        commitBuilder.setAborted(true);
        commitBuilder.setStartTimestamp(startTimestamp);
        builder.setCommitResponse(commitBuilder.build());
        c.write(builder.build());

        abortMeter.mark();

    }

    @Override
    public void sendTimestampResponse(long startTimestamp, Channel c) {

        TSOProto.Response.Builder builder = TSOProto.Response.newBuilder();
        TSOProto.TimestampResponse.Builder respBuilder = TSOProto.TimestampResponse.newBuilder();
        respBuilder.setStartTimestamp(startTimestamp);
        builder.setTimestampResponse(respBuilder.build());
        c.write(builder.build());

        timestampMeter.mark();

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

