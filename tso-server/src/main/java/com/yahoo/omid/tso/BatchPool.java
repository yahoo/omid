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
}