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
package org.apache.omid.tso.client;

import com.google.common.util.concurrent.SettableFuture;

import org.apache.omid.committable.CommitTable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

class MockTSOClient implements TSOProtocol {
    private final AtomicLong timestampGenerator = new AtomicLong();
    private static final int CONFLICT_MAP_SIZE = 1_000_000;
    private final long[] conflictMap = new long[CONFLICT_MAP_SIZE];
    private final Map<Long, Long> fenceMap = new HashMap<Long, Long>();
    private final AtomicLong lwm = new AtomicLong();

    private final CommitTable.Writer commitTable;

    MockTSOClient(CommitTable.Writer commitTable) {
        this.commitTable = commitTable;
    }

    @Override
    public TSOFuture<Long> getNewStartTimestamp() {
        synchronized (conflictMap) {
            SettableFuture<Long> f = SettableFuture.create();
            f.set(timestampGenerator.incrementAndGet());
            return new ForwardingTSOFuture<>(f);
        }
    }

    @Override
    public TSOFuture<Long> getFence(long tableId) {
        synchronized (conflictMap) {
            SettableFuture<Long> f = SettableFuture.create();
            long fenceTimestamp = timestampGenerator.incrementAndGet();
            f.set(fenceTimestamp);
            fenceMap.put(tableId, fenceTimestamp);
            try {
                // Persist the fence by using the fence identifier as both the start and commit timestamp.
                commitTable.addCommittedTransaction(fenceTimestamp, fenceTimestamp);
                commitTable.flush();
            } catch (IOException ioe) {
                f.setException(ioe);
            }
            return new ForwardingTSOFuture<>(f);
        }
    }

    // Checks whether transaction transactionId started before a fence creation of a table transactionId modified.
    private boolean hasConflictsWithFences(long transactionId, Set<? extends CellId> cells) {
        Set<Long> tableIDs = new HashSet<Long>();
        for (CellId c : cells) {
            tableIDs.add(c.getTableId());
        }

        if (! fenceMap.isEmpty()) {
            for (long tableId : tableIDs) {
                Long fence = fenceMap.get(tableId);
                if (fence != null && transactionId < fence) {
                    return true;
                }
                if (fence != null && fence < lwm.get()) { // GC
                    fenceMap.remove(tableId);
                }
            }
        }

        return false;
    }

    // Checks whether transactionId has a write-write conflict with a transaction committed after transactionId.
    private boolean hasConflictsWithCommittedTransactions(long transactionId, Set<? extends CellId> cells) {
        for (CellId c : cells) {
            int index = Math.abs((int) (c.getCellId() % CONFLICT_MAP_SIZE));
            if (conflictMap[index] >= transactionId) {
                return true;
            }
        }

        return false;
    }

    @Override
    public TSOFuture<Long> commit(long transactionId, Set<? extends CellId> cells) {
        synchronized (conflictMap) {
            SettableFuture<Long> f = SettableFuture.create();
            if (transactionId < lwm.get()) {
                f.setException(new AbortException());
                return new ForwardingTSOFuture<>(f);
            }

            if (!hasConflictsWithFences(transactionId, cells) &&
                !hasConflictsWithCommittedTransactions(transactionId, cells)) {

                long commitTimestamp = timestampGenerator.incrementAndGet();
                for (CellId c : cells) {
                    int index = Math.abs((int) (c.getCellId() % CONFLICT_MAP_SIZE));
                    long oldVal = conflictMap[index];
                    conflictMap[index] = commitTimestamp;
                    long curLwm = lwm.get();
                    while (oldVal > curLwm) {
                        if (lwm.compareAndSet(curLwm, oldVal)) {
                            break;
                        }
                        curLwm = lwm.get();
                    }
                }

                f.set(commitTimestamp);
                try {
                    commitTable.addCommittedTransaction(transactionId, commitTimestamp);
                    commitTable.updateLowWatermark(lwm.get());
                    commitTable.flush();
                } catch (IOException ioe) {
                    f.setException(ioe);
                }
            } else {
                f.setException(new AbortException());
            }
            return new ForwardingTSOFuture<>(f);
        }
    }

    @Override
    public TSOFuture<Void> close() {
        SettableFuture<Void> f = SettableFuture.create();
        f.set(null);
        return new ForwardingTSOFuture<>(f);
    }
}
