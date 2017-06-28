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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.omid.metrics.MetricsUtils.name;

/**
 * The Timestamp Oracle that gives monotonically increasing timestamps based on world time
 */
@Singleton
public class WorldClockOracleImpl implements TimestampOracle {

    private static final Logger LOG = LoggerFactory.getLogger(WorldClockOracleImpl.class);

    static final long MAX_TX_PER_MS = 1_000_000; // 1 million
    static final long TIMESTAMP_INTERVAL_MS = 10_000; // 10 seconds interval
    private static final long TIMESTAMP_ALLOCATION_INTERVAL_MS = 7_000; // 7 seconds

    private long lastTimestamp;
    private long maxTimestamp;

    private TimestampStorage storage;
    private Panicker panicker;

    private volatile long maxAllocatedTime;

    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setNameFormat("ts-persist-%d").build());

    private Runnable allocateTimestampsBatchTask;

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

    @Inject
    public WorldClockOracleImpl(MetricsRegistry metrics,
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
        scheduler.schedule(allocateTimestampsBatchTask, 0, TimeUnit.MILLISECONDS);

        // Waiting for the current epoch to start. Occurs in case of failover when the previous TSO allocated the current time frame.
        while ((System.currentTimeMillis() * MAX_TX_PER_MS) < this.lastTimestamp) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
               continue;
            }
        }

        // Launch the periodic timestamp interval allocation. In this case, the timestamp interval is extended even though the TSO is idle.
        // Because we are world time based, this guarantees that the first request after a long time does not need to wait for new interval allocation.
        scheduler.scheduleAtFixedRate(allocateTimestampsBatchTask, TIMESTAMP_ALLOCATION_INTERVAL_MS, TIMESTAMP_ALLOCATION_INTERVAL_MS, TimeUnit.MILLISECONDS);
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

        if (currentMsFirstTimestamp >= maxTimestamp) { // Intentional race to reduce synchronization overhead in every access to maxTimestamp                                                                                                                       
            while (maxAllocatedTime <= currentMsFirstTimestamp) { // Waiting for the interval allocation
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                   continue;
                }
            }
            assert (maxAllocatedTime > maxTimestamp);
            maxTimestamp = maxAllocatedTime;
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
}
