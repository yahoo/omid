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

import org.apache.omid.metrics.MetricsRegistry;
import org.apache.omid.timestamp.storage.TimestampStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;

public class PausableTimestampOracle extends TimestampOracleImpl {

    private static final Logger LOG = LoggerFactory.getLogger(PausableTimestampOracle.class);

    private volatile boolean tsoPaused = false;

    @Inject
    public PausableTimestampOracle(MetricsRegistry metrics,
                                   TimestampStorage tsStorage,
                                   Panicker panicker) throws IOException {
        super(metrics, tsStorage, panicker);
    }

    @Override
    public long next() {
        while (tsoPaused) {
            synchronized (this) {
                try {
                    this.wait();
                } catch (InterruptedException e) {
                    LOG.error("Interrupted whilst paused");
                    Thread.currentThread().interrupt();
                }
            }
        }
        return super.next();
    }

    public synchronized void pause() {
        tsoPaused = true;
        this.notifyAll();
    }

    public synchronized void resume() {
        tsoPaused = false;
        this.notifyAll();
    }

    public boolean isTSOPaused() {
        return tsoPaused;
    }

}
