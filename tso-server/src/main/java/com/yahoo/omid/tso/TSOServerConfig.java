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

import static com.yahoo.omid.tso.PersistenceProcessorImpl.DEFAULT_MAX_BATCH_SIZE;
import static com.yahoo.omid.tso.PersistenceProcessorImpl.TSO_MAX_BATCH_SIZE_KEY;
import static com.yahoo.omid.tso.PersistenceProcessorImpl.DEFAULT_BATCH_PERSIST_TIMEOUT_MS;
import static com.yahoo.omid.tso.PersistenceProcessorImpl.TSO_BATCH_PERSIST_TIMEOUT_MS_KEY;
import static com.yahoo.omid.tso.PersistenceProcessorImpl.DEFAULT_PERSIST_HANDLER_NUM;

import static com.yahoo.omid.tso.RequestProcessorImpl.DEFAULT_MAX_ITEMS;
import static com.yahoo.omid.tso.RequestProcessorImpl.TSO_MAX_ITEMS_KEY;

import javax.inject.Named;
import javax.inject.Singleton;

import com.google.inject.Inject;

@Singleton
public class TSOServerConfig {

    private int maxBatchSize = DEFAULT_MAX_BATCH_SIZE;
    private int batchPersistTimeoutMS = DEFAULT_BATCH_PERSIST_TIMEOUT_MS;
    private int maxItems = DEFAULT_MAX_ITEMS;
    private int persistHandlerNum = DEFAULT_PERSIST_HANDLER_NUM;

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    @Inject(optional=true)
    public void setMaxBatchSize(@Named(TSO_MAX_BATCH_SIZE_KEY) int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }

    public int getBatchPersistTimeoutMS() {
        return batchPersistTimeoutMS;
    }

    @Inject(optional=true)
    public void setBatchPersistTimeoutMS(
            @Named(TSO_BATCH_PERSIST_TIMEOUT_MS_KEY) int batchPersistTimeoutMS) {
        this.batchPersistTimeoutMS = batchPersistTimeoutMS;
    }

    public int getMaxItems() {
        return maxItems;
    }

    public int getPersistHandlerNum() {
        return persistHandlerNum;
    }

    @Inject(optional=true)
    public void setMaxItems(@Named(TSO_MAX_ITEMS_KEY) int maxItems) {
        this.maxItems = maxItems;
    }

}
