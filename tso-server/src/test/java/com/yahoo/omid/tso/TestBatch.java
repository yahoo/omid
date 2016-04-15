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

import com.yahoo.omid.metrics.MetricsRegistry;
import com.yahoo.omid.metrics.NullMetricsProvider;

import org.jboss.netty.channel.Channel;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestBatch {

    private static final Logger LOG = LoggerFactory.getLogger(TestBatch.class);

    private static final int BATCH_SIZE = 1000;
    private MetricsRegistry metrics = new NullMetricsProvider();

    @Mock
    private Channel channel;
    @Mock
    private RetryProcessor retryProcessor;
    @Mock
    private ReplyProcessor replyProcessor;

    // The batch element to test
    private Batch batch;

    @BeforeMethod(alwaysRun = true, timeOut = 30_000)
    public void initMocksAndComponents() {
        MockitoAnnotations.initMocks(this);
        batch = new Batch(BATCH_SIZE);
    }

    @Test
    public void testBatchFunctionality() {

        // Required mocks
        Channel channel = Mockito.mock(Channel.class);
        ReplyProcessor replyProcessor = Mockito.mock(ReplyProcessor.class);
        RetryProcessor retryProcessor = Mockito.mock(RetryProcessor.class);

        // The batch element to test
        Batch batch = new Batch(BATCH_SIZE);

        // Test initial state is OK
        AssertJUnit.assertFalse("Batch shouldn't be full", batch.isFull());
        AssertJUnit.assertEquals("Num events should be 0", 0, batch.getNumEvents());

        // Test adding a single commit event is OK
        MonitoringContext monCtx = new MonitoringContext(metrics);
        monCtx.timerStart("commitPersistProcessor");
        batch.addCommit(0, 1, channel, monCtx);
        AssertJUnit.assertFalse("Batch shouldn't be full", batch.isFull());
        AssertJUnit.assertEquals("Num events should be 1", 1, batch.getNumEvents());

        // Test when filling the batch with events, batch is full
        for (int i = 0; i < (BATCH_SIZE - 1); i++) {
            if (i % 2 == 0) {
                monCtx = new MonitoringContext(metrics);
                monCtx.timerStart("timestampPersistProcessor");
                batch.addTimestamp(i, channel, monCtx);
            } else {
                monCtx = new MonitoringContext(metrics);
                monCtx.timerStart("commitPersistProcessor");
                batch.addCommit(i, i + 1, channel, monCtx);
            }
        }
        assertTrue(batch.isFull(), "Batch should be full");
        assertEquals(BATCH_SIZE, batch.getNumEvents(), "Num events should be " + BATCH_SIZE);

        // Test an exception is thrown when batch is full and a new element is going to be added
        try {
            monCtx = new MonitoringContext(metrics);
            monCtx.timerStart("commitPersistProcessor");
            batch.addCommit(0, 1, channel, new MonitoringContext(metrics));
            Assert.fail("Should throw an IllegalStateException");
        } catch (IllegalStateException e) {
            AssertJUnit.assertEquals("message returned doesn't match", "batch full", e.getMessage());
            LOG.debug("IllegalStateException catched properly");
        }

        // Test that sending replies empties the batch
        final boolean MASTER_INSTANCE = true;

        final boolean SHOULD_MAKE_HEURISTIC_DECISION = true;
        batch.sendReply(replyProcessor, retryProcessor, (-1), MASTER_INSTANCE);
        verify(replyProcessor, timeout(100).times(1))
               .batchResponse(batch, (-1), !SHOULD_MAKE_HEURISTIC_DECISION);
        assertTrue(batch.isFull(), "Batch shouldn't be empty");
    }

    @Test
    public void testBatchFunctionalityWhenMastershipIsLost() {
        Channel channel = Mockito.mock(Channel.class);

        // Fill the batch with events till full
        for (int i = 0; i < BATCH_SIZE; i++) {
            if (i % 2 == 0) {
                MonitoringContext monCtx = new MonitoringContext(metrics);
                monCtx.timerStart("timestampPersistProcessor");
                batch.addTimestamp(i, channel, monCtx);
            } else {
                MonitoringContext monCtx = new MonitoringContext(metrics);
                monCtx.timerStart("commitPersistProcessor");
                batch.addCommit(i, i + 1, channel, monCtx);
            }
        }

        // Test that sending replies empties the batch also when the replica
        // is NOT master and calls the ambiguousCommitResponse() method on the
        // reply processor
        final boolean MASTER_INSTANCE = true;

        final boolean SHOULD_MAKE_HEURISTIC_DECISION = true;
        batch.sendReply(replyProcessor, retryProcessor, (-1), !MASTER_INSTANCE);
        verify(replyProcessor, timeout(100).times(1))
               .batchResponse(batch, (-1), SHOULD_MAKE_HEURISTIC_DECISION);
        assertTrue(batch.isFull(), "Batch should be full");
    }
}
