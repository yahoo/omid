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
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.TimeoutBlockingWaitStrategy;
import com.lmax.disruptor.TimeoutHandler;
import org.apache.omid.metrics.MetricsRegistry;
import org.apache.omid.tso.TSOStateManager.TSOState;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class RequestProcessorImpl implements EventHandler<RequestProcessorImpl.RequestEvent>, RequestProcessor, TimeoutHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RequestProcessorImpl.class);

    private final TimestampOracle timestampOracle;
    private final CommitHashMap hashmap;
    private final MetricsRegistry metrics;
    private final PersistenceProcessor persistProc;
    private final RingBuffer<RequestEvent> requestRing;
    private long lowWatermark = -1L;

    @Inject
    RequestProcessorImpl(MetricsRegistry metrics,
                         TimestampOracle timestampOracle,
                         PersistenceProcessor persistProc,
                         Panicker panicker,
                         TSOServerConfig config)
            throws IOException {

        this.metrics = metrics;

        this.persistProc = persistProc;
        this.timestampOracle = timestampOracle;

        this.hashmap = new CommitHashMap(config.getMaxItems());

        final TimeoutBlockingWaitStrategy timeoutStrategy
                = new TimeoutBlockingWaitStrategy(config.getBatchPersistTimeoutInMs(), TimeUnit.MILLISECONDS);

        // Set up the disruptor thread
        requestRing = RingBuffer.createMultiProducer(RequestEvent.EVENT_FACTORY, 1 << 12, timeoutStrategy);
        SequenceBarrier requestSequenceBarrier = requestRing.newBarrier();
        BatchEventProcessor<RequestEvent> requestProcessor =
                new BatchEventProcessor<>(requestRing, requestSequenceBarrier, this);
        requestRing.addGatingSequences(requestProcessor.getSequence());
        requestProcessor.setExceptionHandler(new FatalExceptionHandler(panicker));

        ExecutorService requestExec = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("request-%d").build());
        // Each processor runs on a separate thread
        requestExec.submit(requestProcessor);

    }

    /**
     * This should be called when the TSO gets leadership
     */
    @Override
    public void update(TSOState state) throws Exception {
        LOG.info("Initializing RequestProcessor...");
        this.lowWatermark = state.getLowWatermark();
        persistProc.persistLowWatermark(lowWatermark).get(); // Sync persist
        LOG.info("RequestProcessor initialized with LWMs {} and Epoch {}", lowWatermark, state.getEpoch());
    }

    @Override
    public void onEvent(RequestEvent event, long sequence, boolean endOfBatch) throws Exception {

        String name = null;
        try { // TODO this should be a switch. Re-check why it's NOT now
            if (event.getType() == RequestEvent.Type.TIMESTAMP) {
                name = "timestampReqProcessor";
                event.getMonCtx().timerStart(name);
                handleTimestamp(event);
            } else if (event.getType() == RequestEvent.Type.COMMIT) {
                name = "commitReqProcessor";
                event.getMonCtx().timerStart(name);
                handleCommit(event);
            }
        } finally {
            if (null != name) {
                event.getMonCtx().timerStop(name);
            }
        }

    }

    @Override
    public void onTimeout(long sequence) throws Exception {

        // TODO We can not use this as a timeout trigger for flushing. This timeout is related to the time between
        // TODO (cont) arrivals of requests to the disruptor. We need another mechanism to trigger timeouts
        // TODO (cont) WARNING!!! Take care with the implementation because if there's other thread than request-0
        // TODO (cont) thread the one that calls persistProc.triggerCurrentBatchFlush(); we'll incur in concurrency issues
        // TODO (cont) This is because, in the current implementation, only the request-0 thread calls the public methods
        // TODO (cont) in persistProc and it is guaranteed that access them serially.
        persistProc.triggerCurrentBatchFlush();

    }

    @Override
    public void timestampRequest(Channel c, MonitoringContext monCtx) {

        long seq = requestRing.next();
        RequestEvent e = requestRing.get(seq);
        RequestEvent.makeTimestampRequest(e, c, monCtx);
        requestRing.publish(seq);

    }

    @Override
    public void commitRequest(long startTimestamp, Collection<Long> writeSet, boolean isRetry, Channel c,
                              MonitoringContext monCtx) {

        long seq = requestRing.next();
        RequestEvent e = requestRing.get(seq);
        RequestEvent.makeCommitRequest(e, startTimestamp, monCtx, writeSet, isRetry, c);
        requestRing.publish(seq);

    }

    private void handleTimestamp(RequestEvent requestEvent) throws Exception {

        long timestamp = timestampOracle.next();
        persistProc.addTimestampToBatch(timestamp, requestEvent.getChannel(), requestEvent.getMonCtx());

    }

    private void handleCommit(RequestEvent event) throws Exception {

        long startTimestamp = event.getStartTimestamp();
        Iterable<Long> writeSet = event.writeSet();
        boolean isCommitRetry = event.isCommitRetry();
        Channel c = event.getChannel();

        boolean txCanCommit;

        int numCellsInWriteset = 0;
        // 0. check if it should abort
        if (startTimestamp <= lowWatermark) {
            txCanCommit = false;
        } else {
            // 1. check the write-write conflicts
            txCanCommit = true;
            for (long cellId : writeSet) {
                long value = hashmap.getLatestWriteForCell(cellId);
                if (value != 0 && value >= startTimestamp) {
                    txCanCommit = false;
                    break;
                }
                numCellsInWriteset++;
            }
        }

        if (txCanCommit) {
            // 2. commit

            long commitTimestamp = timestampOracle.next();

            if (numCellsInWriteset > 0) {
                long newLowWatermark = lowWatermark;

                for (long r : writeSet) {
                    long removed = hashmap.putLatestWriteForCell(r, commitTimestamp);
                    newLowWatermark = Math.max(removed, newLowWatermark);
                }

                if (newLowWatermark != lowWatermark) {
                    LOG.trace("Setting new low Watermark to {}", newLowWatermark);
                    lowWatermark = newLowWatermark;
                    persistProc.persistLowWatermark(newLowWatermark); // Async persist
                }
            }
            persistProc.addCommitToBatch(startTimestamp, commitTimestamp, c, event.getMonCtx());

        } else { // add it to the aborted list
            persistProc.addAbortToBatch(startTimestamp, isCommitRetry, c, event.getMonCtx());
        }

    }

    final static class RequestEvent implements Iterable<Long> {

        enum Type {
            TIMESTAMP, COMMIT
        }

        private Type type = null;
        private Channel channel = null;

        private boolean isCommitRetry = false;
        private long startTimestamp = 0;
        private MonitoringContext monCtx;
        private long numCells = 0;

        private static final int MAX_INLINE = 40;
        private Long writeSet[] = new Long[MAX_INLINE];
        private Collection<Long> writeSetAsCollection = null; // for the case where there's more than MAX_INLINE

        static void makeTimestampRequest(RequestEvent e, Channel c, MonitoringContext monCtx) {
            e.type = Type.TIMESTAMP;
            e.channel = c;
            e.monCtx = monCtx;
        }

        static void makeCommitRequest(RequestEvent e,
                                      long startTimestamp,
                                      MonitoringContext monCtx,
                                      Collection<Long> writeSet,
                                      boolean isRetry,
                                      Channel c) {
            e.monCtx = monCtx;
            e.type = Type.COMMIT;
            e.channel = c;
            e.startTimestamp = startTimestamp;
            e.isCommitRetry = isRetry;
            if (writeSet.size() > MAX_INLINE) {
                e.numCells = writeSet.size();
                e.writeSetAsCollection = writeSet;
            } else {
                e.writeSetAsCollection = null;
                e.numCells = writeSet.size();
                int i = 0;
                for (Long cellId : writeSet) {
                    e.writeSet[i] = cellId;
                    i++;
                }
            }

        }

        MonitoringContext getMonCtx() {
            return monCtx;
        }

        Type getType() {
            return type;
        }

        long getStartTimestamp() {
            return startTimestamp;
        }

        Channel getChannel() {
            return channel;
        }

        @Override
        public Iterator<Long> iterator() {

            if (writeSetAsCollection != null) {
                return writeSetAsCollection.iterator();
            }

            return new Iterator<Long>() {
                int i = 0;

                @Override
                public boolean hasNext() {
                    return i < numCells;
                }

                @Override
                public Long next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    return writeSet[i++];
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };

        }

        Iterable<Long> writeSet() {

            return this;

        }

        boolean isCommitRetry() {
            return isCommitRetry;
        }

        final static EventFactory<RequestEvent> EVENT_FACTORY = new EventFactory<RequestEvent>() {
            @Override
            public RequestEvent newInstance() {
                return new RequestEvent();
            }
        };

    }

}
