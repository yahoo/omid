package com.yahoo.omid.tso;

import com.google.common.base.Optional;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.CommitTable.CommitTimestamp;
import com.yahoo.omid.committable.InMemoryCommitTable;
import com.yahoo.omid.metrics.MetricsRegistry;
import com.yahoo.omid.metrics.NullMetricsProvider;
import org.jboss.netty.channel.Channel;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.testng.AssertJUnit.assertEquals;
import com.yahoo.omid.tso.PersistenceProcessorImpl.PersistenceProcessorHandler.Batch;

public class TestRetryProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(TestRetryProcessor.class);

    private MetricsRegistry metrics = new NullMetricsProvider();

    private static long NON_EXISTING_ST_TX = 1000;
    private static long ST_TX_1 = 0;
    private static long CT_TX_1 = 1;

    @Mock
    private Channel channel;
    @Mock
    private ReplyProcessor replyProc;
    @Mock
    private Panicker panicker;

    private CommitTable commitTable;

    @BeforeMethod(alwaysRun = true, timeOut = 30_000)
    public void initMocksAndComponents() {
        MockitoAnnotations.initMocks(this);
        // Init components
        commitTable = new InMemoryCommitTable();
        metrics = new NullMetricsProvider();
    }

    @Test(timeOut = 10_000)
    public void testBasicFunctionality() throws Exception {

        commitTable.getWriter().addCommittedTransaction(ST_TX_1, CT_TX_1);

        // The element to test
        RetryProcessor retryProc = new RetryProcessorImpl(metrics, commitTable, replyProc, panicker);

        // Test we'll reply with an abort for a retry request when the start timestamp IS NOT in the commit table
        retryProc.disambiguateRetryRequestHeuristically(NON_EXISTING_ST_TX, channel, new MonitoringContext(metrics));
        ArgumentCaptor<Long> firstTScapture = ArgumentCaptor.forClass(Long.class);

        verify(replyProc, timeout(100).times(1)).addAbort(any(Batch.class), firstTScapture.capture(), any(Channel.class), any(MonitoringContext.class));
        long startTS = firstTScapture.getValue();
        assertEquals("Captured timestamp should be the same as NON_EXISTING_ST_TX", NON_EXISTING_ST_TX, startTS);

        // Test we'll reply with a commit for a retry request when the start timestamp IS in the commit table
        commitTable.getWriter().addCommittedTransaction(ST_TX_1, CT_TX_1); // Add a tx to commit table

        retryProc.disambiguateRetryRequestHeuristically(ST_TX_1, channel, new MonitoringContext(metrics));
        ArgumentCaptor<Long> secondTScapture = ArgumentCaptor.forClass(Long.class);

        verify(replyProc, timeout(100).times(1))
                        .addCommit(any(Batch.class), firstTScapture.capture(), secondTScapture.capture(), any(Channel.class), any(MonitoringContext.class));

        startTS = firstTScapture.getValue();
        long commitTS = secondTScapture.getValue();
        assertEquals("Captured timestamp should be the same as ST_TX_1", ST_TX_1, startTS);
        assertEquals("Captured timestamp should be the same as CT_TX_1", CT_TX_1, commitTS);
    }

    @Test(timeOut = 10_000)
    public void testRetriedRequestForInvalidatedTransactionReturnsAnAbort() throws Exception {

        // Invalidate the transaction
        commitTable.getClient().tryInvalidateTransaction(ST_TX_1);

        // Pre-start verification: Validate that the transaction is invalidated
        // NOTE: This test should be in the a test class for InMemoryCommitTable
        Optional<CommitTimestamp> invalidTxMarker = commitTable.getClient().getCommitTimestamp(ST_TX_1).get();
        Assert.assertTrue(invalidTxMarker.isPresent());
        Assert.assertEquals(invalidTxMarker.get().getValue(), InMemoryCommitTable.INVALID_TRANSACTION_MARKER);

        // The element to test
        RetryProcessor retryProc = new RetryProcessorImpl(metrics, commitTable, replyProc, panicker);

        // Test we'll reply with an abort for a retry request when the
        // transaction id IS in the commit table BUT invalidated
        retryProc.disambiguateRetryRequestHeuristically(ST_TX_1, channel, new MonitoringContext(metrics));
        ArgumentCaptor<Long> startTScapture = ArgumentCaptor.forClass(Long.class);
        verify(replyProc, timeout(100).times(1)).addAbort(any(Batch.class), startTScapture.capture(), any(Channel.class), any(MonitoringContext.class));
        long startTS = startTScapture.getValue();
        Assert.assertEquals(startTS, ST_TX_1, "Captured timestamp should be the same as NON_EXISTING_ST_TX");

    }

}

