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

import org.apache.omid.committable.CommitTable;
import org.apache.omid.metrics.MetricsRegistry;
import org.apache.omid.metrics.NullMetricsProvider;
import org.apache.omid.tso.PersistenceProcessorImpl.PersistBatchEvent;
import org.jboss.netty.channel.Channel;
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
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestPersistenceProcessorHandler {

    private static final Logger LOG = LoggerFactory.getLogger(TestPersistenceProcessorHandler.class);

    private static final int BATCH_ID = 0;
    private static final int BATCH_SIZE = 6;
    private static final long BATCH_SEQUENCE = 0;

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
    private CommitTable.Writer mockWriter;
    @Mock
    private CommitTable.Client mockClient;
    @Mock
    private LeaseManager leaseManager;
    @Mock
    private ReplyProcessor replyProcessor;
    @Mock
    private RetryProcessor retryProcessor;
    @Mock
    private Panicker panicker;

    private CommitTable commitTable;

    private MetricsRegistry metrics;

    // Component under test
    private PersistenceProcessorHandler persistenceHandler;

    @BeforeMethod(alwaysRun = true, timeOut = 30_000)
    public void initMocksAndComponents() throws Exception {

        MockitoAnnotations.initMocks(this);

        // Configure null metrics provider
        metrics = new NullMetricsProvider();

        // Configure commit table to return the mocked writer and client
        commitTable = new CommitTable() {
            @Override
            public Writer getWriter() {
                return mockWriter;
            }

            @Override
            public Client getClient() {
                return mockClient;
            }
        };

        // Simulate we're master for most of the tests
        doReturn(true).when(leaseManager).stillInLeasePeriod();

        persistenceHandler = spy(new PersistenceProcessorHandler(metrics,
                                                                 "localhost:1234",
                                                                 leaseManager,
                                                                 commitTable,
                                                                 replyProcessor,
                                                                 retryProcessor,
                                                                 panicker));

    }

    @AfterMethod
    void afterMethod() {
        Mockito.reset(mockWriter);
    }

    @Test(timeOut = 10_000)
    public void testProcessingOfEmptyBatchPersistEvent() throws Exception {

        // Prepare test batch
        Batch batch = new Batch(BATCH_ID, BATCH_SIZE);
        PersistBatchEvent batchEvent = new PersistBatchEvent();
        PersistBatchEvent.makePersistBatch(batchEvent, BATCH_SEQUENCE, batch);
        persistenceHandler.onEvent(batchEvent);

        verify(persistenceHandler, times(1)).flush(eq(0));
        verify(persistenceHandler, times(1)).filterAndDissambiguateClientRetries(eq(batch));
        verify(retryProcessor, never()).disambiguateRetryRequestHeuristically(anyLong(), any(Channel.class), any(MonitoringContext.class));
        verify(replyProcessor, times(1)).manageResponsesBatch(eq(BATCH_SEQUENCE), eq(batch));
        assertTrue(batch.isEmpty());

    }

    @Test(timeOut = 10_000)
    public void testProcessingOfBatchPersistEventWithASingleTimestampEvent() throws Exception {

        // Prepare test batch
        Batch batch = new Batch(BATCH_ID, BATCH_SIZE);
        batch.addTimestamp(FIRST_ST, null, mock(MonitoringContext.class));
        PersistBatchEvent batchEvent = new PersistBatchEvent();
        PersistBatchEvent.makePersistBatch(batchEvent, BATCH_SEQUENCE, batch);
        persistenceHandler.onEvent(batchEvent);

        verify(persistenceHandler, times(1)).flush(eq(0));
        verify(persistenceHandler, times(1)).filterAndDissambiguateClientRetries(eq(batch));
        verify(retryProcessor, never()).disambiguateRetryRequestHeuristically(anyLong(), any(Channel.class), any(MonitoringContext.class));
        verify(replyProcessor, times(1)).manageResponsesBatch(eq(BATCH_SEQUENCE), eq(batch));
        assertEquals(batch.getNumEvents(), 1);
        assertEquals(batch.get(0).getStartTimestamp(), FIRST_ST);

    }

    @Test(timeOut = 10_000)
    public void testProcessingOfBatchPersistEventWithASingleCommitEvent() throws Exception {

        // Prepare test batch
        Batch batch = new Batch(BATCH_ID, BATCH_SIZE);
        batch.addCommit(FIRST_ST, FIRST_CT, null, mock(MonitoringContext.class));
        PersistBatchEvent batchEvent = new PersistBatchEvent();
        PersistBatchEvent.makePersistBatch(batchEvent, BATCH_SEQUENCE, batch);
        persistenceHandler.onEvent(batchEvent);

        verify(persistenceHandler, times(1)).flush(eq(1));
        verify(persistenceHandler, times(1)).filterAndDissambiguateClientRetries(batch);
        verify(retryProcessor, never()).disambiguateRetryRequestHeuristically(anyLong(), any(Channel.class), any(MonitoringContext.class));
        verify(replyProcessor, times(1)).manageResponsesBatch(eq(BATCH_SEQUENCE), eq(batch));
        assertEquals(batch.getNumEvents(), 1);
        assertEquals(batch.get(0).getStartTimestamp(), FIRST_ST);
        assertEquals(batch.get(0).getCommitTimestamp(), FIRST_CT);

    }

    @Test(timeOut = 10_000)
    public void testProcessingOfBatchPersistEventWithASingleAbortEventNoRetry() throws Exception {

        final boolean IS_RETRY = false;

        // Prepare test batch
        Batch batch = new Batch(BATCH_ID, BATCH_SIZE);
        batch.addAbort(FIRST_ST, IS_RETRY, null, mock(MonitoringContext.class));
        PersistBatchEvent batchEvent = new PersistBatchEvent();
        PersistBatchEvent.makePersistBatch(batchEvent, BATCH_SEQUENCE, batch);
        persistenceHandler.onEvent(batchEvent);

        verify(persistenceHandler, times(1)).flush(eq(0));
        verify(persistenceHandler, times(1)).filterAndDissambiguateClientRetries(batch);
        verify(retryProcessor, never()).disambiguateRetryRequestHeuristically(anyLong(), any(Channel.class), any(MonitoringContext.class));
        verify(replyProcessor, times(1)).manageResponsesBatch(eq(BATCH_SEQUENCE), eq(batch));
        assertEquals(batch.getNumEvents(), 1);
        assertEquals(batch.get(0).getStartTimestamp(), FIRST_ST);

    }

    @Test(timeOut = 10_000)
    public void testProcessingOfBatchPersistEventWithASingleAbortEventWithRetry() throws Exception {

        final boolean IS_RETRY = true;

        // Prepare test batch
        Batch batch = new Batch(BATCH_ID, BATCH_SIZE);
        batch.addAbort(FIRST_ST, IS_RETRY, null, mock(MonitoringContext.class));
        PersistBatchEvent batchEvent = new PersistBatchEvent();
        PersistBatchEvent.makePersistBatch(batchEvent, BATCH_SEQUENCE, batch);

        // Call process method
        persistenceHandler.onEvent(batchEvent);

        verify(persistenceHandler, times(1)).flush(eq(0));
        verify(persistenceHandler, times(1)).filterAndDissambiguateClientRetries(batch);
        verify(retryProcessor, times(1)).disambiguateRetryRequestHeuristically(eq(FIRST_ST), any(Channel.class), any(MonitoringContext.class));
        verify(replyProcessor, times(1)).manageResponsesBatch(eq(BATCH_SEQUENCE), eq(batch));
        assertEquals(batch.getNumEvents(), 0);

    }

    @Test(timeOut = 10_000)
    public void testProcessingOfBatchPersistEventWith2EventsCommitAndAbortWithRetry() throws Exception {

        final boolean IS_RETRY = true;

        // Prepare test batch
        Batch batch = new Batch(BATCH_ID, BATCH_SIZE);
        batch.addCommit(FIRST_ST, FIRST_CT, null, mock(MonitoringContext.class));
        batch.addAbort(SECOND_ST, IS_RETRY, null, mock(MonitoringContext.class));
        PersistBatchEvent batchEvent = new PersistBatchEvent();
        PersistBatchEvent.makePersistBatch(batchEvent, BATCH_SEQUENCE, batch);

        // Initial assertion
        assertEquals(batch.getNumEvents(), 2);

        // Call process method
        persistenceHandler.onEvent(batchEvent);

        verify(persistenceHandler, times(1)).flush(eq(1));
        verify(persistenceHandler, times(1)).filterAndDissambiguateClientRetries(eq(batch));
        verify(retryProcessor, times(1)).disambiguateRetryRequestHeuristically(eq(SECOND_ST), any(Channel.class), any(MonitoringContext.class));
        verify(replyProcessor, times(1)).manageResponsesBatch(eq(BATCH_SEQUENCE), eq(batch));
        assertEquals(batch.getNumEvents(), 1);
        assertEquals(batch.get(0).getStartTimestamp(), FIRST_ST);
        assertEquals(batch.get(0).getCommitTimestamp(), FIRST_CT);

    }

    @Test(timeOut = 10_000)
    public void testProcessingOfBatchPersistEventWith2EventsAbortWithRetryAndCommit() throws Exception {
        // ------------------------------------------------------------------------------------------------------------
        // Same test as testProcessingOfBatchPersistEventWith2EventsCommitAndAbortWithRetry but swapped events
        // ------------------------------------------------------------------------------------------------------------

        final boolean IS_RETRY = true;

        // Prepare test batch
        Batch batch = new Batch(BATCH_ID, BATCH_SIZE);
        batch.addAbort(FIRST_ST, IS_RETRY, null, mock(MonitoringContext.class));
        batch.addCommit(SECOND_ST, SECOND_CT, null, mock(MonitoringContext.class));
        PersistBatchEvent batchEvent = new PersistBatchEvent();
        PersistBatchEvent.makePersistBatch(batchEvent, BATCH_SEQUENCE, batch);

        // Initial assertion
        assertEquals(batch.getNumEvents(), 2);

        // Call process method
        persistenceHandler.onEvent(batchEvent);

        verify(persistenceHandler, times(1)).flush(eq(1));
        verify(persistenceHandler, times(1)).filterAndDissambiguateClientRetries(eq(batch));
        verify(retryProcessor, times(1)).disambiguateRetryRequestHeuristically(eq(FIRST_ST), any(Channel.class), any(MonitoringContext.class));
        verify(replyProcessor, times(1)).manageResponsesBatch(eq(BATCH_SEQUENCE), eq(batch));
        assertEquals(batch.getNumEvents(), 1);
        assertEquals(batch.get(0).getStartTimestamp(), SECOND_ST);
        assertEquals(batch.get(0).getCommitTimestamp(), SECOND_CT);

    }

    @Test(timeOut = 10_000)
    public void testProcessingOfBatchPersistEventWith2AbortWithRetryEvents() throws Exception {

        final boolean IS_RETRY = true;

        // Prepare test batch
        Batch batch = new Batch(BATCH_ID, BATCH_SIZE);
        batch.addAbort(FIRST_ST, IS_RETRY, null, mock(MonitoringContext.class));
        batch.addAbort(SECOND_ST, IS_RETRY, null, mock(MonitoringContext.class));
        PersistBatchEvent batchEvent = new PersistBatchEvent();
        PersistBatchEvent.makePersistBatch(batchEvent, BATCH_SEQUENCE, batch);

        // Initial assertion
        assertEquals(batch.getNumEvents(), 2);

        // Call process method
        persistenceHandler.onEvent(batchEvent);

        verify(persistenceHandler, times(1)).flush(eq(0));
        verify(persistenceHandler, times(1)).filterAndDissambiguateClientRetries(eq(batch));
        verify(retryProcessor, times(1)).disambiguateRetryRequestHeuristically(eq(FIRST_ST), any(Channel.class), any(MonitoringContext.class));
        verify(retryProcessor, times(1)).disambiguateRetryRequestHeuristically(eq(SECOND_ST), any(Channel.class), any(MonitoringContext.class));
        verify(replyProcessor, times(1)).manageResponsesBatch(eq(BATCH_SEQUENCE), eq(batch));
        assertEquals(batch.getNumEvents(), 0);

    }

    @Test(timeOut = 10_000)
    public void testProcessingOfBatchPersistEventWith2NonRetryAbortEvents() throws Exception {

        final boolean IS_RETRY = false;

        // Prepare test batch
        Batch batch = new Batch(BATCH_ID, BATCH_SIZE);
        batch.addAbort(FIRST_ST, IS_RETRY, null, mock(MonitoringContext.class));
        batch.addAbort(SECOND_ST, IS_RETRY, null, mock(MonitoringContext.class));
        PersistBatchEvent batchEvent = new PersistBatchEvent();
        PersistBatchEvent.makePersistBatch(batchEvent, BATCH_SEQUENCE, batch);

        // Initial assertion
        assertEquals(batch.getNumEvents(), 2);

        // Call process method
        persistenceHandler.onEvent(batchEvent);

        verify(persistenceHandler, times(1)).flush(eq(0));
        verify(persistenceHandler, times(1)).filterAndDissambiguateClientRetries(eq(batch));
        verify(retryProcessor, never()).disambiguateRetryRequestHeuristically(anyLong(), any(Channel.class), any(MonitoringContext.class));
        verify(replyProcessor, times(1)).manageResponsesBatch(eq(BATCH_SEQUENCE), eq(batch));
        assertEquals(batch.getNumEvents(), 2);
        assertEquals(batch.get(0).getStartTimestamp(), FIRST_ST);
        assertEquals(batch.get(1).getStartTimestamp(), SECOND_ST);

    }


    @Test(timeOut = 10_000)
    public void testProcessingOfBatchPersistEventWithMultipleRetryAndNonRetryEvents() throws Exception {

        // Prepare test batch
        Batch batch = new Batch(BATCH_ID, BATCH_SIZE);

        batch.addTimestamp(FIRST_ST, null, mock(MonitoringContext.class));
        batch.addAbort(SECOND_ST, true, null, mock(MonitoringContext.class));
        batch.addCommit(THIRD_ST, THIRD_CT, null, mock(MonitoringContext.class));
        batch.addAbort(FOURTH_ST, false, null, mock(MonitoringContext.class));
        batch.addCommit(FIFTH_ST, FIFTH_CT, null, mock(MonitoringContext.class));
        batch.addAbort(SIXTH_ST, true, null, mock(MonitoringContext.class));
        PersistBatchEvent batchEvent = new PersistBatchEvent();
        PersistBatchEvent.makePersistBatch(batchEvent, BATCH_SEQUENCE, batch);

        // Initial assertion
        assertEquals(batch.getNumEvents(), 6);

        // Call process method
        persistenceHandler.onEvent(batchEvent);

        verify(persistenceHandler, times(1)).flush(2); // 2 commits to flush
        verify(persistenceHandler, times(1)).filterAndDissambiguateClientRetries(eq(batch));
        verify(retryProcessor, times(1)).disambiguateRetryRequestHeuristically(eq(SECOND_ST), any(Channel.class), any(MonitoringContext.class));
        verify(replyProcessor, times(1)).manageResponsesBatch(eq(BATCH_SEQUENCE), eq(batch));
        assertEquals(batch.getNumEvents(), 4);
        assertEquals(batch.get(0).getStartTimestamp(), FIRST_ST);
        assertEquals(batch.get(1).getStartTimestamp(), FIFTH_ST);
        assertEquals(batch.get(1).getCommitTimestamp(), FIFTH_CT);
        assertEquals(batch.get(2).getStartTimestamp(), THIRD_ST);
        assertEquals(batch.get(2).getCommitTimestamp(), THIRD_CT);
        assertEquals(batch.get(3).getStartTimestamp(), FOURTH_ST);

    }

    @Test(timeOut = 10_000)
    public void testPanicPersistingEvents() throws Exception {

        // User the real panicker
        Panicker panicker = spy(new RuntimeExceptionPanicker());
        persistenceHandler = spy(new PersistenceProcessorHandler(metrics,
                                                                 "localhost:1234",
                                                                 leaseManager,
                                                                 commitTable,
                                                                 replyProcessor,
                                                                 retryProcessor,
                                                                 panicker));

        // Prepare test batch
        Batch batch = new Batch(BATCH_ID, BATCH_SIZE);
        batch.addCommit(FIRST_ST, FIRST_CT, null, mock(MonitoringContext.class));
        PersistBatchEvent batchEvent = new PersistBatchEvent();
        PersistBatchEvent.makePersistBatch(batchEvent, BATCH_SEQUENCE, batch);

        doThrow(IOException.class).when(mockWriter).flush();

        try {
            persistenceHandler.onEvent(batchEvent);
            fail();
        } catch (RuntimeException re) {
            // Expected
        }

        verify(persistenceHandler, times(1)).flush(1);
        verify(panicker, times(1)).panic(eq("Error persisting commit batch"), any(IOException.class));
        verify(persistenceHandler, never()).filterAndDissambiguateClientRetries(any(Batch.class));
        verify(replyProcessor, never()).manageResponsesBatch(anyLong(), any(Batch.class));

    }

    @Test(timeOut = 10_000)
    public void testPanicBecauseMasterLosesMastership() throws Exception {

        // ------------------------------------------------------------------------------------------------------------
        // 1) Test panic before flushing
        // ------------------------------------------------------------------------------------------------------------

        // Simulate we lose mastership BEFORE flushing
        doReturn(false).when(leaseManager).stillInLeasePeriod();

        // User the real panicker
        Panicker panicker = spy(new RuntimeExceptionPanicker());
        persistenceHandler = spy(new PersistenceProcessorHandler(metrics,
                                                                 "localhost:1234",
                                                                 leaseManager,
                                                                 commitTable,
                                                                 replyProcessor,
                                                                 retryProcessor,
                                                                 panicker));

        // Prepare test batch
        Batch batch = new Batch(BATCH_ID, BATCH_SIZE);
        batch.addCommit(FIRST_ST, FIRST_CT, null, mock(MonitoringContext.class));
        PersistBatchEvent batchEvent = new PersistBatchEvent();
        PersistBatchEvent.makePersistBatch(batchEvent, BATCH_SEQUENCE, batch);

        try {
            persistenceHandler.onEvent(batchEvent);
            fail();
        } catch (RuntimeException re) {
            // Expected
        }
        verify(persistenceHandler, times(1)).flush(eq(1));
        verify(mockWriter, never()).flush();
        verify(panicker, times(1)).panic(eq("Replica localhost:1234 lost mastership whilst flushing data. Committing suicide"), any(IOException.class));
        verify(persistenceHandler, never()).filterAndDissambiguateClientRetries(any(Batch.class));
        verify(replyProcessor, never()).manageResponsesBatch(anyLong(), any(Batch.class));

        // ------------------------------------------------------------------------------------------------------------
        // 2) Test panic after flushing
        // ------------------------------------------------------------------------------------------------------------

        // Simulate we lose mastership AFTER flushing
        doReturn(true).doReturn(false).when(leaseManager).stillInLeasePeriod();

        // User the real panicker
        panicker = spy(new RuntimeExceptionPanicker());
        persistenceHandler = spy(new PersistenceProcessorHandler(metrics,
                                                                 "localhost:1234",
                                                                 leaseManager,
                                                                 commitTable,
                                                                 replyProcessor,
                                                                 retryProcessor,
                                                                 panicker));

        // Prepare test batch
        batch = new Batch(BATCH_ID, BATCH_SIZE);
        batch.addCommit(FIRST_ST, FIRST_CT, null, mock(MonitoringContext.class));
        batchEvent = new PersistBatchEvent();
        PersistBatchEvent.makePersistBatch(batchEvent, BATCH_SEQUENCE, batch);

        try {
            persistenceHandler.onEvent(batchEvent);
            fail();
        } catch (RuntimeException re) {
            // Expected
        }
        verify(persistenceHandler, times(1)).flush(eq(1));
        verify(mockWriter, times(1)).flush();
        verify(panicker, times(1)).panic(eq("Replica localhost:1234 lost mastership whilst flushing data. Committing suicide"), any(IOException.class));
        verify(persistenceHandler, never()).filterAndDissambiguateClientRetries(any(Batch.class));
        verify(replyProcessor, never()).manageResponsesBatch(anyLong(), any(Batch.class));

    }

}
