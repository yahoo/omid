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

import org.jboss.netty.channel.Channel;

import com.yahoo.omid.tso.PersistEvent.Type;

public class Batch {
    final PersistEvent[] events;
    final int maxBatchSize;
    int numEvents;
    volatile boolean avaialble;

    Batch(int maxBatchSize) {
        assert (maxBatchSize > 0);
        this.maxBatchSize = maxBatchSize;
        events = new PersistEvent[maxBatchSize];
        numEvents = 0;
        for (int i = 0; i < maxBatchSize; i++) {
            events[i] = new PersistEvent();
        }
        avaialble = true;
    }

    boolean isAvailable() {
        return avaialble;
    }

    void setNotAvailable() {
        avaialble = false;
    }

    void setAvailable() {
        avaialble = true;
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
        setAvailable();
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
                PersistEvent tmp = events[i];
                events[i] = events[numEvents - 1];
                events[numEvents - 1] = tmp;
                if (numEvents == 1) {
                    clear();
                    reply.batchResponse(null, batchID, !isTSOInstanceMaster);
                    return;
                }
                --numEvents;
                continue;
            }
            ++i;
        }

        reply.batchResponse(this, batchID, !isTSOInstanceMaster);
    }
}
