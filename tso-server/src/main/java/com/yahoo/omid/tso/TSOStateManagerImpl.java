/**
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
package com.yahoo.omid.tso;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Implements the management of the state of the TSO
 */
public class TSOStateManagerImpl implements TSOStateManager {

    private static final Logger LOG = LoggerFactory.getLogger(TSOStateManagerImpl.class);

    private List<StateObserver> stateObservers = new ArrayList<>();

    private TSOState state;

    private TimestampOracle timestampOracle;

    @Inject
    public TSOStateManagerImpl(TimestampOracle timestampOracle) {
        this.timestampOracle = timestampOracle;
    }

    @Override
    public synchronized void register(StateObserver newObserver) {
        Preconditions.checkNotNull(newObserver, "Trying to register a null observer");
        if (!stateObservers.contains(newObserver)) {
            stateObservers.add(newObserver);
        }
    }

    @Override
    public synchronized void unregister(StateObserver observer) {
        stateObservers.remove(observer);
    }

    @Override
    public synchronized TSOState reset() throws IOException, InterruptedException {
        LOG.info("Reseting the TSO Server state...");
        // The timestamp oracle dictates the new state
        timestampOracle.initialize();
        long lowWatermark = timestampOracle.getLast();
        // In this implementation the epoch == low watermark
        long epoch = lowWatermark;
        state = new TSOState(lowWatermark, epoch);

        // Then, notify registered observers about the new state
        for (StateObserver stateObserver : stateObservers) {
            stateObserver.update(state);
        }
        LOG.info("New TSO Server state {}", state);
        return state;
    }

}
