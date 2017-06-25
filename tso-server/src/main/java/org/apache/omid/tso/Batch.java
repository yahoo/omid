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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class Batch {

    private static final Logger LOG = LoggerFactory.getLogger(Batch.class);

    private final int id;
    private final int size;
    private int numEvents;
    private final PersistEvent[] events; // TODO Check if it's worth to have a dynamic structure for this

    Batch(int id, int size) {

        Preconditions.checkArgument(size > 0, "Size [%s] must be positive", size);
        this.size = size;
        this.id = id;
        this.numEvents = 0;
        this.events = new PersistEvent[size];
        for (int i = 0; i < size; i++) {
            this.events[i] = new PersistEvent();
        }
        LOG.info("Batch id {} created with size {}", id, size);

    }

    PersistEvent get(int idx) {
        Preconditions.checkState(numEvents > 0 && 0 <= idx && idx < numEvents,
                                 "Accessing Events array (Size = %s) with wrong index [%s]", numEvents, idx);
        return events[idx];
    }

    void set(int idx, PersistEvent event) {
        Preconditions.checkState(0 <= idx && idx < numEvents);
        events[idx] = event;
    }

    void clear() {

        numEvents = 0;

    }

    void decreaseNumEvents() {
        numEvents--;
    }

    int getNumEvents() {
        return numEvents;
    }

    int getLastEventIdx() {
        return numEvents - 1;
    }

    boolean isFull() {

        Preconditions.checkState(numEvents <= size, "Batch Full: numEvents [%s] > size [%s]", numEvents, size);
        return numEvents == size;

    }

    boolean isEmpty() {

        return numEvents == 0;

    }

    void addTimestamp(long startTimestamp, Channel c, MonitoringContext context) {

        Preconditions.checkState(!isFull(), "batch is full");
        int index = numEvents++;
        PersistEvent e = events[index];
        context.timerStart("persistence.processor.timestamp.latency");
        e.makePersistTimestamp(startTimestamp, c, context);

    }

    void addFence(long tableID, long fenceTimestamp, Channel c, MonitoringContext context) {

        Preconditions.checkState(!isFull(), "batch is full");
        int index = numEvents++;
        PersistEvent e = events[index];
        context.timerStart("persistence.processor.fence.latency");
        e.makePersistFence(tableID, fenceTimestamp, c, context);

    }

    void addCommit(long startTimestamp, long commitTimestamp, Channel c, MonitoringContext context) {

        Preconditions.checkState(!isFull(), "batch is full");
        int index = numEvents++;
        PersistEvent e = events[index];
        context.timerStart("persistence.processor.commit.latency");
        e.makePersistCommit(startTimestamp, commitTimestamp, c, context);

    }

    void addCommitRetry(long startTimestamp, Channel c, MonitoringContext context) {

        Preconditions.checkState(!isFull(), "batch is full");
        int index = numEvents++;
        PersistEvent e = events[index];
        context.timerStart("persistence.processor.commit-retry.latency");
        e.makeCommitRetry(startTimestamp, c, context);

    }

    void addAbort(long startTimestamp, Channel c, MonitoringContext context) {

        Preconditions.checkState(!isFull(), "batch is full");
        int index = numEvents++;
        PersistEvent e = events[index];
        context.timerStart("persistence.processor.abort.latency");
        e.makePersistAbort(startTimestamp, c, context);

    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("id", id)
                .add("size", size)
                .add("num events", numEvents)
                .add("events", Arrays.toString(events))
                .toString();
    }

    static class BatchFactory extends BasePooledObjectFactory<Batch> {

        private static int batchId = 0;

        private int batchSize;

        BatchFactory(int batchSize) {
            this.batchSize = batchSize;
        }

        @Override
        public Batch create() throws Exception {
            return new Batch(batchId++, batchSize);
        }

        @Override
        public PooledObject<Batch> wrap(Batch batch) {
            return new DefaultPooledObject<>(batch);
        }

        @Override
        public void passivateObject(PooledObject<Batch> pooledObject) {
            pooledObject.getObject().clear(); // Reset num events when returning the batch to the pool
        }

    }

}
