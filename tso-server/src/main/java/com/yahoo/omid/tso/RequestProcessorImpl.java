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
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.TimeoutBlockingWaitStrategy;
import com.lmax.disruptor.TimeoutHandler;
import com.yahoo.omid.metrics.MetricsRegistry;
import com.yahoo.omid.tso.TSOStateManager.TSOState;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RequestProcessorImpl implements EventHandler<RequestProcessorImpl.RequestEvent>,
                                             RequestProcessor, TimeoutHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RequestProcessorImpl.class);

    static final int DEFAULT_MAX_ITEMS = 1_000_000;

    private final TimestampOracle timestampOracle;
    public final CommitHashMap hashmap;
    private final MetricsRegistry metrics;
    private final PersistenceProcessor persistProc;
    private final RingBuffer<RequestEvent> requestRing;
    private long lowWatermark = -1L;
    private long epoch = -1L;

    @Inject
    RequestProcessorImpl(MetricsRegistry metrics,
                         TimestampOracle timestampOracle,
                         PersistenceProcessor persistProc,
                         Panicker panicker,
                         TSOServerConfig config) throws IOException {
        this.metrics = metrics;

        this.persistProc = persistProc;
        this.timestampOracle = timestampOracle;

        this.hashmap = new CommitHashMap(config.getMaxItems());

        final TimeoutBlockingWaitStrategy timeoutStrategy
        = new TimeoutBlockingWaitStrategy(config.getBatchPersistTimeoutInMs(), TimeUnit.MILLISECONDS);

        // Set up the disruptor thread
        requestRing = RingBuffer.<RequestEvent>createMultiProducer(RequestEvent.EVENT_FACTORY, 1 << 12,
                                                                   timeoutStrategy);
        SequenceBarrier requestSequenceBarrier = requestRing.newBarrier();
        BatchEventProcessor<RequestEvent> requestProcessor =
                new BatchEventProcessor<RequestEvent>(requestRing,
                        requestSequenceBarrier,
                        this);
        requestRing.addGatingSequences(requestProcessor.getSequence());
        requestProcessor.setExceptionHandler(new FatalExceptionHandler(panicker));

        ExecutorService requestExec = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("request-%d").build());
        // Each processor runs on a separate thread
        requestExec.submit(requestProcessor);

    }

    /**
     * This should be called when the TSO gets initialized or gets leadership
     */
    @Override
    public void update(TSOState state) throws IOException {
        LOG.info("Reseting RequestProcessor...");
        this.lowWatermark = state.getLowWatermark();
        persistProc.persistLowWatermark(lowWatermark, new MonitoringContext(metrics));
        this.epoch = state.getEpoch();
        hashmap.reset();
        persistProc.reset();
        LOG.info("RequestProcessor initialized with LWM {} and Epoch {}", lowWatermark, epoch);
    }

    @Override
    public void onEvent(RequestEvent event, long sequence, boolean endOfBatch) throws Exception {
        String name = null;
        try {
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
            if (name != null) {
                event.getMonCtx().timerStop(name);
            }
        }

        if (endOfBatch) {
            persistProc.persistFlush();
        }
    }

    @Override
    public void onTimeout(long sequence) throws Exception {
        persistProc.persistFlush();
    }

    @Override
    public void timestampRequest(Channel c, MonitoringContext monCtx) {
        long seq = requestRing.next();
        RequestEvent e = requestRing.get(seq);
        RequestEvent.makeTimestampRequest(e, c, monCtx);
        requestRing.publish(seq);
    }

    @Override
    public void commitRequest(long startTimestamp, Collection<Long> writeSet, boolean isRetry, Channel c, MonitoringContext monCtx) {
        long seq = requestRing.next();
        RequestEvent e = requestRing.get(seq);
        RequestEvent.makeCommitRequest(e, startTimestamp, monCtx, writeSet, isRetry, c);
        requestRing.publish(seq);
    }

    public void handleTimestamp(RequestEvent requestEvent) {
        long timestamp;

        try {
            timestamp = timestampOracle.next();
        } catch (IOException e) {
            LOG.error("Error getting timestamp", e);
            return;
        }

        persistProc.persistTimestamp(timestamp, requestEvent.getChannel(), requestEvent.getMonCtx());
    }

    public long handleCommit(RequestEvent event) {
        long startTimestamp = event.getStartTimestamp();
        Iterable<Long> writeSet = event.writeSet();
        boolean isRetry = event.isRetry();
        Channel c = event.getChannel();

        boolean committed = false;
        long commitTimestamp = 0L;

        int numCellsInWriteset = 0;
        // 0. check if it should abort
        if (startTimestamp <= lowWatermark) {
            committed = false;
        } else {
            // 1. check the write-write conflicts
            committed = true;
            for (long cellId : writeSet) {
                long value = hashmap.getLatestWriteForCell(cellId);
                if (value != 0 && value >= startTimestamp) {
                    committed = false;
                    break;
                }
                numCellsInWriteset++;
            }
        }

        if (committed) {
            // 2. commit
            try {
                commitTimestamp = timestampOracle.next();

                if (numCellsInWriteset > 0) {
                    long newLowWatermark = lowWatermark;

                    for (long r : writeSet) {
                        long removed = hashmap.putLatestWriteForCell(r, commitTimestamp);
                        newLowWatermark = Math.max(removed, newLowWatermark);
                    }

                    if (newLowWatermark != lowWatermark) {
                        LOG.trace("Setting new low Watermark to {}", newLowWatermark);
                        lowWatermark = newLowWatermark;
                        persistProc.persistLowWatermark(newLowWatermark, event.getMonCtx());
                    }
                }
                persistProc.persistCommit(startTimestamp, commitTimestamp, c, event.getMonCtx());
            } catch (IOException e) {
                LOG.error("Error committing", e);
            }
        } else { // add it to the aborted list
            persistProc.persistAbort(startTimestamp, isRetry, c, event.getMonCtx());
        }

        return commitTimestamp;
    }

    final static class RequestEvent implements Iterable<Long> {

        enum Type {
            TIMESTAMP, COMMIT
        }

        ;

        private Type type = null;
        private Channel channel = null;

        private boolean isRetry = false;
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
                                      long startTimestamp, MonitoringContext monCtx, Collection<Long> writeSet,
                                      boolean isRetry, Channel c) {
            e.monCtx = monCtx;
            e.type = Type.COMMIT;
            e.channel = c;
            e.startTimestamp = startTimestamp;
            e.isRetry = isRetry;
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

        boolean isRetry() {
            return isRetry;
        }

        public final static EventFactory<RequestEvent> EVENT_FACTORY
        = new EventFactory<RequestEvent>() {
            @Override
            public RequestEvent newInstance() {
                return new RequestEvent();
            }
        };
    }
};
