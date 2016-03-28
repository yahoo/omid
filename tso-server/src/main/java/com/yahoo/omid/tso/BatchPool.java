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

import javax.inject.Inject;
import org.jboss.netty.channel.Channel;

import com.yahoo.omid.tso.PersistEvent.Type;

public class BatchPool {
    final private Batch[] batches;
    final private int poolSize;
    private int emptyBatch;

    @Inject
    public BatchPool(TSOServerConfig config) {
        poolSize = config.getPersistHandlerNum() * config.getNumBuffersPerHandler();
        batches = new Batch[poolSize];
        for (int i=0; i < poolSize; ++i) {
            batches[i] = new Batch(config.getMaxBatchSize() / config.getPersistHandlerNum());
        }
        emptyBatch = 0;
    }

    public Batch getNextEmptyBatch() {
        for (;batches[emptyBatch].getNumEvents() != 0; emptyBatch = (emptyBatch + 1) % (poolSize));
        return batches[emptyBatch];
    }

    public void reset() {
        for (int i=0; i < poolSize; ++i) {
            batches[i].clear();
        }
        emptyBatch = 0;
    }


    public static class Batch {
        final PersistEvent[] events;
        final int maxBatchSize;
        int numEvents;


        Batch(int maxBatchSize) {
            assert (maxBatchSize > 0);
            this.maxBatchSize = maxBatchSize;
            events = new PersistEvent[maxBatchSize];
            numEvents = 0;
            for (int i = 0; i < maxBatchSize; i++) {
                events[i] = new PersistEvent();
            }
        }

        boolean isFull() {
            assert (numEvents <= maxBatchSize);
            return numEvents == maxBatchSize;
        }

        boolean isEmpty() {
            return numEvents == 0;
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
            e.makePersistCommit(startTimestamp, commitTimestamp, c, context);
        }

        void addAbort(long startTimestamp, boolean isRetry, Channel c, MonitoringContext context) {
            if (isFull()) {
                throw new IllegalStateException("batch full");
            }
            int index = numEvents++;
            PersistEvent e = events[index];
            e.makePersistAbort(startTimestamp, isRetry, c, context);
        }

        void addUndecidedRetriedRequest(long startTimestamp, Channel c, MonitoringContext context) {
            if (isFull()) {
                throw new IllegalStateException("batch full");
            }
            int index = numEvents++;
            PersistEvent e = events[index];
            // We mark the event as an ABORT retry to identify the events to send
            // to the retry processor
            e.makePersistAbort(startTimestamp, true, c, context);
        }

        void addTimestamp(long startTimestamp, Channel c, MonitoringContext context) {
            if (isFull()) {
                throw new IllegalStateException("batch full");
            }
            int index = numEvents++;
            PersistEvent e = events[index];
            e.makePersistTimestamp(startTimestamp, c, context);
        }

        void addLowWatermark(long lowWatermark, MonitoringContext context) {
            if (isFull()) {
                throw new IllegalStateException("batch full");
            }
            int index = numEvents++;
            PersistEvent e = events[index];
            e.makePersistLowWatermark(lowWatermark, context);
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