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
import org.apache.omid.metrics.Gauge;
import org.apache.omid.metrics.MetricsRegistry;
import org.apache.omid.timestamp.storage.TimestampStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.apache.omid.metrics.MetricsUtils.name;

/**
 * The Timestamp Oracle that gives monotonically increasing timestamps based on world time
 */
@Singleton
public class CurrentTimestampOracleImpl implements TimestampOracle {

    private static final Logger LOG = LoggerFactory.getLogger(CurrentTimestampOracleImpl.class);

    @VisibleForTesting
    static class InMemoryTimestampStorage implements TimestampStorage {

        long maxTime = 0;

        @Override
        public void updateMaxTimestamp(long previousMaxTime, long nextMaxTime) {
            maxTime = nextMaxTime;
            LOG.info("Updating max timestamp: (previous:{}, new:{})", previousMaxTime, nextMaxTime);
        }

        @Override
        public long getMaxTimestamp() {
            return maxTime;
        }

    }

    private class AllocateTimestampBatchTask implements Runnable {
        long previousMaxTime;

        AllocateTimestampBatchTask(long previousMaxTime) {
            this.previousMaxTime = previousMaxTime;
        }

        @Override
        public void run() {
            long newMaxTime = (System.currentTimeMillis() + TIMESTAMP_INTERVAL_MS) * MAX_TX_PER_MS;
            try {
                storage.updateMaxTimestamp(previousMaxTime, newMaxTime);
                maxAllocatedTime = newMaxTime;
                previousMaxTime = newMaxTime;
            } catch (Throwable e) {
                panicker.panic("Can't store the new max timestamp", e);
            }
        }
    }

    static final long MAX_TX_PER_MS = 1_000_000; // 1 million
    private static final long TIMESTAMP_REMAINING_THRESHOLD = 3_000 * MAX_TX_PER_MS; // max number of transactions in 3 seconds
    private static final long TIMESTAMP_INTERVAL_MS = 10_000; // 10 seconds interval

    private long lastTimestamp;
    private long maxTimestamp;

    private TimestampStorage storage;
    private Panicker panicker;

    private long nextAllocationThreshold;
    private volatile long maxAllocatedTime;

    private Executor executor = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("ts-persist-%d").build());

    private Runnable allocateTimestampsBatchTask;

    @Inject
    public CurrentTimestampOracleImpl(MetricsRegistry metrics,
                               TimestampStorage tsStorage,
                               Panicker panicker) throws IOException {

        this.storage = tsStorage;
        this.panicker = panicker;

        metrics.gauge(name("tso", "maxTimestamp"), new Gauge<Long>() {
            @Override
            public Long getValue() {
                return maxTimestamp;
            }
        });

    }

    @Override
    public void initialize() throws IOException {

        this.lastTimestamp = this.maxTimestamp = storage.getMaxTimestamp();

        this.allocateTimestampsBatchTask = new AllocateTimestampBatchTask(lastTimestamp);

        // Trigger first allocation of timestamps
        executor.execute(allocateTimestampsBatchTask);

        // Waiting for the current epoch to start. Occurs in case of fallback when the previous TSO allocated the current time frame.
        while ((System.currentTimeMillis() * MAX_TX_PER_MS) < this.lastTimestamp);
    }

    /**
     * Returns the next timestamp if available. Otherwise spins till the ts-persist thread allocates a new timestamp.
     */
    @Override
    public long next() {

        long currentMsFirstTimestamp = System.currentTimeMillis() * MAX_TX_PER_MS;

        // Return the next timestamp in case we are still in the same millisecond as the previous timestamp was. 
        if (++lastTimestamp >= currentMsFirstTimestamp) {
            return lastTimestamp;
        }

        if (currentMsFirstTimestamp >= nextAllocationThreshold) {
                nextAllocationThreshold = Long.MAX_VALUE; // guarantees that only one will enter this branch (this is a sequential code so it should work)
                executor.execute(allocateTimestampsBatchTask);
        }

        if (currentMsFirstTimestamp >= maxTimestamp) {
            while (maxAllocatedTime <= currentMsFirstTimestamp);
            assert (maxAllocatedTime > maxTimestamp);
            maxTimestamp = maxAllocatedTime;
            nextAllocationThreshold = maxTimestamp - TIMESTAMP_REMAINING_THRESHOLD;
        }

        lastTimestamp = currentMsFirstTimestamp;

        return lastTimestamp;
    }

    @Override
    public long getLast() {
        return lastTimestamp;
    }

    @Override
    public String toString() {
        return String.format("TimestampOracle -> LastTimestamp: %d, MaxTimestamp: %d", lastTimestamp, maxTimestamp);
    }

}
