package com.yahoo.omid.tso;

import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.metrics.MetricsRegistry;
import com.yahoo.omid.metrics.NullMetricsProvider;
import com.yahoo.omid.tso.BatchPool.Batch;

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
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;


public class TestPersistenceProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(TestPersistenceProcessor.class);

    @Mock
    private Batch batch;
    @Mock
    private CommitTable.Writer mockWriter;
    @Mock
    private CommitTable.Client mockClient;
    @Mock
    private RetryProcessor retryProcessor;
    @Mock
    private ReplyProcessor replyProcessor;
    @Mock
    private Panicker panicker;

    private MetricsRegistry metrics;
    private CommitTable commitTable;

    @BeforeMethod(alwaysRun = true, timeOut = 30_000)
    public void initMocksAndComponents() throws Exception {

        MockitoAnnotations.initMocks(this);

        // Configure mock writer to flush successfully
        doThrow(new IOException("Unable to write")).when(mockWriter).flush();

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
    }

    @AfterMethod
    void afterMethod() {
        Mockito.reset(mockWriter);
    }

    @Test
    public void testCommitPersistenceWithMultiHandlers() throws Exception {

        // Init a non-HA lease manager
        VoidLeaseManager leaseManager = spy(new VoidLeaseManager(mock(TSOChannelHandler.class),
                mock(TSOStateManager.class)));

        TSOServerConfig tsoConfig = new TSOServerConfig();
        tsoConfig.setPersistHandlerNum(4);

        // Component under test
        PersistenceProcessorImpl proc = new PersistenceProcessorImpl(tsoConfig,
                                                                 metrics,
                                                                 new BatchPool(tsoConfig),
                                                                 "localhost:1234",
                                                                 leaseManager,
                                                                 commitTable,
                                                                 replyProcessor,
                                                                 retryProcessor,
                                                                 panicker);

        MonitoringContext monCtx = new MonitoringContext(metrics);
        proc.batch = batch;
        proc.persistCommit(1, 2, null, monCtx);
        proc.persistFlush();
        proc.batch = batch;
        proc.persistCommit(3, 4, null, monCtx);
        proc.persistFlush();
        proc.batch = batch;
        proc.persistCommit(5, 6, null, monCtx);
        proc.persistFlush();
        proc.batch = batch;
        proc.persistCommit(7, 8, null, monCtx);
        proc.persistFlush();
        verify(batch, timeout(1000).times(4)).sendReply(any(ReplyProcessor.class),
                                                        any(RetryProcessor.class),
                                                        any(Long.class), eq(true));
    }

    @Test
    public void testCommitPersistenceWithSingleHanlderInMultiHandlersEnvironment() throws Exception {

        // Init a non-HA lease manager
        VoidLeaseManager leaseManager = spy(new VoidLeaseManager(mock(TSOChannelHandler.class),
                mock(TSOStateManager.class)));

        TSOServerConfig tsoConfig = new TSOServerConfig();
        tsoConfig.setPersistHandlerNum(4);

        // Component under test
        PersistenceProcessorImpl proc = new PersistenceProcessorImpl(tsoConfig,
                                                                 metrics,
                                                                 new BatchPool(tsoConfig),
                                                                 "localhost:1234",
                                                                 leaseManager,
                                                                 commitTable,
                                                                 replyProcessor,
                                                                 retryProcessor,
                                                                 panicker);

        MonitoringContext monCtx = new MonitoringContext(metrics);
        proc.batch = batch;
        proc.persistCommit(1, 2, null, monCtx);

        proc.persistCommit(3, 4, null, monCtx);
        proc.persistCommit(5, 6, null, monCtx);
        proc.persistCommit(7, 8, null, monCtx);
        verify(batch, timeout(1000).times(0)).sendReply(any(ReplyProcessor.class),
                any(RetryProcessor.class),
                any(Long.class), eq(true));
        proc.persistFlush();
        verify(batch, timeout(1000).times(1)).sendReply(any(ReplyProcessor.class),
                                                        any(RetryProcessor.class),
                                                        any(Long.class), eq(true));
        proc.batch = batch;
        proc.persistCommit(1, 2, null, monCtx);
        proc.persistCommit(3, 4, null, monCtx);
        proc.persistCommit(5, 6, null, monCtx);
        proc.persistCommit(7, 8, null, monCtx);
        verify(batch, timeout(1000).times(1)).sendReply(any(ReplyProcessor.class),
                any(RetryProcessor.class),
                any(Long.class), eq(true));
        proc.persistFlush();
        verify(batch, timeout(1000).times(2)).sendReply(any(ReplyProcessor.class),
                any(RetryProcessor.class),
                any(Long.class), eq(true));
        proc.batch = batch;

        // Test empty flush does not trigger response
        proc.persistFlush();
        verify(batch, timeout(1000).times(2)).sendReply(any(ReplyProcessor.class),
                any(RetryProcessor.class),
                any(Long.class), eq(true));
    }

    @Test
    public void testCommitPersistenceWithNonHALeaseManager() throws Exception {

        TSOServerConfig tsoConfig = new TSOServerConfig();

        // Init a non-HA lease manager
        VoidLeaseManager leaseManager = spy(new VoidLeaseManager(mock(TSOChannelHandler.class),
                mock(TSOStateManager.class)));

        // Component under test
        PersistenceProcessorImpl proc = new PersistenceProcessorImpl(tsoConfig,
                                                                 metrics,
                                                                 new BatchPool(tsoConfig),
                                                                 "localhost:1234",
                                                                 leaseManager,
                                                                 commitTable,
                                                                 replyProcessor,
                                                                 retryProcessor,
                                                                 panicker);


        // The non-ha lease manager always return true for
        // stillInLeasePeriod(), so verify the batch sends replies as master
        MonitoringContext monCtx = new MonitoringContext(metrics);
        proc.batch = batch;
        proc.persistCommit(1, 2, null, monCtx);
        proc.persistFlush();
        verify(leaseManager, timeout(1000).times(2)).stillInLeasePeriod();
        verify(batch, timeout(1000).times(1)).sendReply(any(ReplyProcessor.class),
                                                        any(RetryProcessor.class),
                                                        any(Long.class), eq(true));
    }

    @Test
    public void testCommitPersistenceWithHALeaseManagerMultiHandlers() throws Exception {
        TSOServerConfig tsoConfig = new TSOServerConfig();
        tsoConfig.setPersistHandlerNum(4);

        testCommitPersistenceWithHALeaseManagerPerConfig(tsoConfig);
    }

    @Test
    public void testCommitPersistenceWithHALeaseManager() throws Exception {
        testCommitPersistenceWithHALeaseManagerPerConfig(new TSOServerConfig());
    }

    private void testCommitPersistenceWithHALeaseManagerPerConfig (TSOServerConfig tsoConfig) throws Exception {

        // Init a HA lease manager
        LeaseManager leaseManager = mock(LeaseManager.class);

        // Component under test
        PersistenceProcessorImpl proc = new PersistenceProcessorImpl(tsoConfig,
                                                                     metrics,
                                                                     new BatchPool(tsoConfig),
                                                                     "localhost:1234",
                                                                     leaseManager,
                                                                     commitTable,
                                                                     replyProcessor,
                                                                     retryProcessor,
                                                                     panicker);

        // Configure the lease manager to always return true for
        // stillInLeasePeriod, so verify the batch sends replies as master
        doReturn(true).when(leaseManager).stillInLeasePeriod();
        MonitoringContext monCtx = new MonitoringContext(metrics);
        proc.batch = batch;
        proc.persistCommit(1, 2, null, monCtx);
        proc.persistFlush();
        verify(leaseManager, timeout(1000).times(2)).stillInLeasePeriod();
        verify(batch, timeout(1000).times(1)).sendReply(any(ReplyProcessor.class), any(RetryProcessor.class), any(Long.class), eq(true));

        // Configure the lease manager to always return true first and false
        // later for stillInLeasePeriod, so verify the batch sends replies as
        // non-master
        reset(leaseManager);
        reset(batch);
        proc.batch = batch;
        doReturn(true).doReturn(false).when(leaseManager).stillInLeasePeriod();
        proc.persistCommit(1, 2, null, monCtx);
        proc.persistFlush();
        verify(leaseManager, timeout(1000).times(2)).stillInLeasePeriod();
        verify(batch, timeout(1000).times(1)).sendReply(any(ReplyProcessor.class), any(RetryProcessor.class), any(Long.class), eq(false));

        // Configure the lease manager to always return false for
        // stillInLeasePeriod, so verify the batch sends replies as non-master
        reset(leaseManager);
        reset(batch);
        proc.batch = batch;
        doReturn(false).when(leaseManager).stillInLeasePeriod();
        proc.persistCommit(1, 2, null, monCtx);
        proc.persistFlush();
        verify(leaseManager, timeout(1000).times(1)).stillInLeasePeriod();
        verify(batch, timeout(1000).times(1)).sendReply(any(ReplyProcessor.class), any(RetryProcessor.class), any(Long.class), eq(false));
    }

    @Test
    public void testCommitTableExceptionOnCommitPersistenceTakesDownDaemon() throws Exception {

        // Init lease management (doesn't matter if HA or not)
        LeaseManagement leaseManager = mock(LeaseManagement.class);
        TSOServerConfig config = new TSOServerConfig();
        BatchPool batchPool = new BatchPool(config);
        PersistenceProcessorImpl proc = new PersistenceProcessorImpl(config,
                                                                 metrics,
                                                                 batchPool,
                                                                 "localhost:1234",
                                                                 leaseManager,
                                                                 commitTable,
                                                                 mock(ReplyProcessor.class),
                                                                 mock(RetryProcessor.class),
                                                                 panicker);

        MonitoringContext monCtx = new MonitoringContext(metrics);

        // Configure lease manager to work normally
        doReturn(true).when(leaseManager).stillInLeasePeriod();

        // Configure commit table writer to explode when flushing changes to DB
        doThrow(new IOException("Unable to write@TestPersistenceProcessor2")).when(mockWriter).flush();

        // Check the panic is extended!
        proc.persistCommit(1, 2, null, monCtx);
        proc.persistFlush();
        verify(panicker, timeout(1000).atLeastOnce()).panic(anyString(), any(Throwable.class));
    }

    @Test
    public void testRuntimeExceptionOnCommitPersistenceTakesDownDaemon() throws Exception {

        TSOServerConfig config = new TSOServerConfig();
        BatchPool batchPool = new BatchPool(config);

        PersistenceProcessorImpl proc = new PersistenceProcessorImpl(config,
                                                                 metrics,
                                                                 batchPool,
                                                                 "localhost:1234",
                                                                 mock(LeaseManagement.class),
                                                                 commitTable,
                                                                 replyProcessor,
                                                                 retryProcessor,
                                                                 panicker);

        // Configure writer to explode with a runtime exception
        doThrow(new RuntimeException("Kaboom!")).when(mockWriter).addCommittedTransaction(anyLong(), anyLong());
        MonitoringContext monCtx = new MonitoringContext(metrics);

        // Check the panic is extended!
        proc.persistCommit(1, 2, null, monCtx);
        proc.persistFlush();
        verify(panicker, timeout(1000).atLeastOnce()).panic(anyString(), any(Throwable.class));
    }

}
