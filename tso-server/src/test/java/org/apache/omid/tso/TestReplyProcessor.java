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

import org.apache.commons.pool2.ObjectPool;
import org.apache.omid.metrics.MetricsRegistry;
import org.apache.omid.metrics.NullMetricsProvider;
import org.apache.omid.tso.PersistenceProcessorImpl.PersistBatchEvent;
import org.apache.omid.tso.ReplyProcessorImpl.ReplyBatchEvent;
import org.jboss.netty.channel.Channel;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEqualsNoOrder;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestReplyProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(TestReplyProcessor.class);

    private static final long ANY_DISRUPTOR_SEQUENCE = 1234L;

    public static final int BATCH_POOL_SIZE = 3;

    private static final long FIRST_ST = 0L;
    private static final long FIRST_CT = 1L;
    private static final long SECOND_ST = 2L;
    private static final long SECOND_CT = 3L;
    private static final long THIRD_ST = 4L;
    private static final long THIRD_CT = 5L;
    private static final long FOURTH_ST = 6L;
    private static final long FOURTH_CT = 7L;
    private static final long FIFTH_ST = 8L;
    private static final long FIFTH_CT = 9L;
    private static final long SIXTH_ST = 10L;

    @Mock
    private Panicker panicker;

    private MetricsRegistry metrics;

    private ObjectPool<Batch> batchPool;

    // Component under test
    private ReplyProcessorImpl replyProcessor;

    @BeforeMethod(alwaysRun = true, timeOut = 30_000)
    public void initMocksAndComponents() throws Exception {

        MockitoAnnotations.initMocks(this);

        TSOServerConfig tsoConfig = new TSOServerConfig();
        tsoConfig.setNumConcurrentCTWriters(BATCH_POOL_SIZE);

        // Configure null metrics provider
        metrics = new NullMetricsProvider();

        batchPool = spy(new BatchPoolModule(tsoConfig).getBatchPool());

        replyProcessor = spy(new ReplyProcessorImpl(metrics, panicker, batchPool));

    }

    @AfterMethod
    void afterMethod() {
    }

    @Test(timeOut = 10_000)
    public void testBadFormedPackageThrowsException() throws Exception {

        // We need an instance throwing exceptions for this test
        replyProcessor = spy(new ReplyProcessorImpl(metrics, new RuntimeExceptionPanicker(), batchPool));

        // Prepare test batch
        Batch batch = batchPool.borrowObject();
        batch.addCommitRetry(FIRST_ST, null, null  );
        ReplyBatchEvent e = ReplyBatchEvent.EVENT_FACTORY.newInstance();
        ReplyBatchEvent.makeReplyBatch(e, batch, 0);

        assertEquals(replyProcessor.nextIDToHandle.get(), 0);
        assertEquals(replyProcessor.futureEvents.size(), 0);
        assertEquals(batchPool.getNumActive(), 1);
        assertEquals(batchPool.getNumIdle(), BATCH_POOL_SIZE - 1);

        try {
            replyProcessor.onEvent(e, ANY_DISRUPTOR_SEQUENCE, false);
            fail();
        } catch(RuntimeException re) {
            // Expected
        }

        assertEquals(replyProcessor.nextIDToHandle.get(), 0);
        assertEquals(replyProcessor.futureEvents.size(), 0);
        assertEquals(batchPool.getNumActive(), 1);
        assertEquals(batchPool.getNumIdle(), BATCH_POOL_SIZE - 1);

    }

    @Test(timeOut = 10_000)
    public void testUnorderedBatchSequenceGetsSaved() throws Exception {

        final long HIGH_SEQUENCE_NUMBER = 1234L; // Should be greater than 0

        // Prepare test batch
        Batch batch = batchPool.borrowObject();
        ReplyBatchEvent e = ReplyBatchEvent.EVENT_FACTORY.newInstance();
        ReplyBatchEvent.makeReplyBatch(e, batch, HIGH_SEQUENCE_NUMBER);

        assertEquals(replyProcessor.nextIDToHandle.get(), 0);
        assertEquals(replyProcessor.futureEvents.size(), 0);
        assertEquals(batchPool.getNumActive(), 1);
        assertEquals(batchPool.getNumIdle(), BATCH_POOL_SIZE - 1);

        replyProcessor.onEvent(e, ANY_DISRUPTOR_SEQUENCE, false);

        assertEquals(replyProcessor.nextIDToHandle.get(), 0);
        assertEquals(replyProcessor.futureEvents.size(), 1);
        assertEquals(batchPool.getNumActive(), 1);
        assertEquals(batchPool.getNumIdle(), BATCH_POOL_SIZE - 1);
        assertTrue(batch.isEmpty());
        verify(replyProcessor, times(0)).handleReplyBatchEvent(any(ReplyBatchEvent.class));

    }

    @Test(timeOut = 10_000)
    public void testProcessingOfEmptyBatchReplyEvent() throws Exception {

        // Prepare test batch
        Batch batch = batchPool.borrowObject();
        ReplyBatchEvent e = ReplyBatchEvent.EVENT_FACTORY.newInstance();
        ReplyBatchEvent.makeReplyBatch(e, batch, 0);

        assertEquals(replyProcessor.nextIDToHandle.get(), 0);
        assertEquals(replyProcessor.futureEvents.size(), 0);
        assertEquals(batchPool.getNumActive(), 1);
        assertEquals(batchPool.getNumIdle(), BATCH_POOL_SIZE - 1);

        replyProcessor.onEvent(e, ANY_DISRUPTOR_SEQUENCE, false);

        assertEquals(replyProcessor.nextIDToHandle.get(), 1);
        assertEquals(replyProcessor.futureEvents.size(), 0);
        assertEquals(batchPool.getNumActive(), 0);
        assertEquals(batchPool.getNumIdle(), BATCH_POOL_SIZE);
        assertTrue(batch.isEmpty());
        verify(replyProcessor, times(1)).handleReplyBatchEvent(eq(e));

    }

    @Test(timeOut = 10_000)
    public void testUnorderedSequenceOfBatchReplyEventsThatMustBeOrderedBeforeSendingReplies() throws Exception {

        // Prepare 3 batches with events and simulate a different order of arrival using the batch sequence

        // Prepare first a delayed batch (Batch #3)
        Batch thirdBatch = batchPool.borrowObject();
        thirdBatch.addTimestamp(FIRST_ST, mock(Channel.class), mock(MonitoringContext.class));
        thirdBatch.addCommit(SECOND_ST, SECOND_CT, mock(Channel.class), mock(MonitoringContext.class));
        ReplyBatchEvent thirdBatchEvent = ReplyBatchEvent.EVENT_FACTORY.newInstance();
        ReplyBatchEvent.makeReplyBatch(thirdBatchEvent, thirdBatch, 2); // Set a higher sequence than the initial one

        assertEquals(replyProcessor.nextIDToHandle.get(), 0);
        assertEquals(replyProcessor.futureEvents.size(), 0);
        assertEquals(batchPool.getNumActive(), 1);
        assertEquals(batchPool.getNumIdle(), BATCH_POOL_SIZE - 1);

        replyProcessor.onEvent(thirdBatchEvent, ANY_DISRUPTOR_SEQUENCE, false);

        assertEquals(replyProcessor.nextIDToHandle.get(), 0);
        assertEquals(replyProcessor.futureEvents.size(), 1);
        assertEquals(batchPool.getNumActive(), 1);
        assertEquals(batchPool.getNumIdle(), BATCH_POOL_SIZE - 1);
        assertFalse(thirdBatch.isEmpty());
        verify(replyProcessor, never()).handleReplyBatchEvent(eq(thirdBatchEvent));

        // Prepare another delayed batch (Batch #2)
        Batch secondBatch = batchPool.borrowObject();
        secondBatch.addTimestamp(THIRD_ST, mock(Channel.class), mock(MonitoringContext.class));
        secondBatch.addCommit(FOURTH_ST, FOURTH_CT, mock(Channel.class), mock(MonitoringContext.class));
        ReplyBatchEvent secondBatchEvent = ReplyBatchEvent.EVENT_FACTORY.newInstance();
        ReplyBatchEvent.makeReplyBatch(secondBatchEvent, secondBatch, 1); // Set another higher sequence

        replyProcessor.onEvent(secondBatchEvent, ANY_DISRUPTOR_SEQUENCE, false);

        assertEquals(replyProcessor.nextIDToHandle.get(), 0);
        assertEquals(replyProcessor.futureEvents.size(), 2);
        assertEquals(batchPool.getNumActive(), 2);
        assertEquals(batchPool.getNumIdle(), BATCH_POOL_SIZE - 2);
        assertFalse(secondBatch.isEmpty());
        assertFalse(thirdBatch.isEmpty());

        // Finally, prepare the batch that should trigger the execution of the other two
        Batch firstBatch = batchPool.borrowObject();
        firstBatch.addAbort(FIFTH_ST, mock(Channel.class), mock(MonitoringContext.class));
        ReplyBatchEvent firstBatchEvent = ReplyBatchEvent.EVENT_FACTORY.newInstance();
        ReplyBatchEvent.makeReplyBatch(firstBatchEvent, firstBatch, 0); // Set the first batch with a higher sequence

        replyProcessor.onEvent(firstBatchEvent, ANY_DISRUPTOR_SEQUENCE, false);

        assertEquals(replyProcessor.nextIDToHandle.get(), 3);
        assertEquals(replyProcessor.futureEvents.size(), 0);
        assertEquals(batchPool.getNumActive(), 0);
        assertEquals(batchPool.getNumIdle(), BATCH_POOL_SIZE);
        assertTrue(firstBatch.isEmpty());
        assertTrue(secondBatch.isEmpty());
        assertTrue(thirdBatch.isEmpty());

        // Check the method calls have been properly ordered

        InOrder inOrderReplyBatchEvents = inOrder(replyProcessor, replyProcessor, replyProcessor);
        inOrderReplyBatchEvents.verify(replyProcessor, times(1)).handleReplyBatchEvent(eq(firstBatchEvent));
        inOrderReplyBatchEvents.verify(replyProcessor, times(1)).handleReplyBatchEvent(eq(secondBatchEvent));
        inOrderReplyBatchEvents.verify(replyProcessor, times(1)).handleReplyBatchEvent(eq(thirdBatchEvent));

        InOrder inOrderReplies = inOrder(replyProcessor, replyProcessor, replyProcessor, replyProcessor, replyProcessor);
        inOrderReplies.verify(replyProcessor, times(1)).sendAbortResponse(eq(FIFTH_ST), any(Channel.class));
        inOrderReplies.verify(replyProcessor, times(1)).sendTimestampResponse(eq(THIRD_ST), any(Channel.class));
        inOrderReplies.verify(replyProcessor, times(1)).sendCommitResponse(eq(FOURTH_ST), eq(FOURTH_CT), any(Channel.class));
        inOrderReplies.verify(replyProcessor, times(1)).sendTimestampResponse(eq(FIRST_ST), any(Channel.class));
        inOrderReplies.verify(replyProcessor, times(1)).sendCommitResponse(eq(SECOND_ST), eq(SECOND_CT), any(Channel.class));

    }

}
