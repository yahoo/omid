package com.yahoo.omid.tso;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.metrics.MetricsRegistry;
import com.yahoo.omid.metrics.NullMetricsProvider;
import com.yahoo.omid.timestamp.storage.TimestampStorage;

public class TestPanicker {

    private static final Logger LOG = LoggerFactory.getLogger(TestPanicker.class);
    private final CommitTable.Writer mockWriter = mock(CommitTable.Writer.class);
    private MetricsRegistry metrics = new NullMetricsProvider();

    @AfterMethod
    void afterMethod(){
        Mockito.reset(mockWriter);
    }

    // Note this test has been moved and refactored to TestTimestampOracle because
    // it tests the behaviour of the TimestampOracle.
    // Please, remove me in a future commit
    @Test
    public void testTimestampOraclePanic() throws Exception {
        TimestampStorage storage = spy(new TimestampOracleImpl.InMemoryTimestampStorage());
        Panicker panicker = spy(new MockPanicker());

        doThrow(new RuntimeException("Out of memory or something"))
            .when(storage).updateMaxTimestamp(anyLong(), anyLong());

        final TimestampOracleImpl tso = new TimestampOracleImpl(metrics,
                storage, panicker);
        tso.initialize();
        Thread allocThread = new Thread("AllocThread") {
                @Override
                public void run() {
                    try {
                        while (true) {
                            tso.next();
                        }
                    } catch (IOException ioe) {
                        LOG.error("Shouldn't occur");
                    }
                }
            };
        allocThread.start();

        verify(panicker, timeout(1000).atLeastOnce()).panic(anyString(), any(Throwable.class));
    }

    // Note this test has been moved and refactored to TestPersistenceProcessor because
    // it tests the behaviour of the PersistenceProcessor.
    // Please, remove me in a future commit
    @Test
    public void testCommitTablePanic() throws Exception {
        Panicker panicker = spy(new MockPanicker());

        doThrow(new IOException("Unable to write@TestPanicker")).when(mockWriter).flush();

        final CommitTable.Client mockClient = mock(CommitTable.Client.class);
        CommitTable commitTable = new CommitTable() {
                @Override
                public ListenableFuture<Writer> getWriter() {
                    SettableFuture<Writer> f = SettableFuture.<Writer>create();
                    f.set(mockWriter);
                    return f;
                }

                @Override
                public ListenableFuture<Client> getClient() {
                    SettableFuture<Client> f = SettableFuture.<Client>create();
                    f.set(mockClient);
                    return f;
                }
            };

        LeaseManager leaseManager = mock(LeaseManager.class);
        doReturn(true).when(leaseManager).stillInLeasePeriod();
        PersistenceProcessor proc = new PersistenceProcessorImpl(metrics,
                                                                 "localhost:1234",
                                                                 leaseManager,
                                                                 commitTable,
                                                                 mock(ReplyProcessor.class),
                                                                 mock(RetryProcessor.class),
                                                                 panicker,
                                                                 new TSOServerConfig());
        proc.persistCommit(1, 2, null, new MonitoringContext(metrics));

        new RequestProcessorImpl(metrics, mock(TimestampOracle.class), proc, panicker, mock(TSOServerConfig.class));

        verify(panicker, timeout(1000).atLeastOnce()).panic(anyString(), any(Throwable.class));
    }

    // Note this test has been moved and refactored to TestPersistenceProcessor because
    // it tests the behaviour of the PersistenceProcessor.
    // Please, remove me in a future commit
    @Test
    public void testRuntimeExceptionTakesDownDaemon() throws Exception {
        Panicker panicker = spy(new MockPanicker());

        final CommitTable.Writer mockWriter = mock(CommitTable.Writer.class);
        doThrow(new RuntimeException("Kaboom!"))
            .when(mockWriter).addCommittedTransaction(anyLong(),anyLong());

        final CommitTable.Client mockClient = mock(CommitTable.Client.class);
        CommitTable commitTable = new CommitTable() {
                @Override
                public ListenableFuture<Writer> getWriter() {
                    SettableFuture<Writer> f = SettableFuture.<Writer>create();
                    f.set(mockWriter);
                    return f;
                }

                @Override
                public ListenableFuture<Client> getClient() {
                    SettableFuture<Client> f = SettableFuture.<Client>create();
                    f.set(mockClient);
                    return f;
                }
            };
        PersistenceProcessor proc = new PersistenceProcessorImpl(metrics,
                                                                 "localhost:1234",
                                                                 mock(LeaseManager.class),
                                                                 commitTable,
                                                                 mock(ReplyProcessor.class),
                                                                 mock(RetryProcessor.class),
                                                                 panicker,
                                                                 new TSOServerConfig());
        proc.persistCommit(1, 2, null, new MonitoringContext(metrics));

        new RequestProcessorImpl(metrics, mock(TimestampOracle.class), proc, panicker, mock(TSOServerConfig.class));

        verify(panicker, timeout(1000).atLeastOnce()).panic(anyString(), any(Throwable.class));
    }
}
