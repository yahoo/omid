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
import org.apache.omid.committable.CommitTable;
import org.apache.omid.metrics.MetricsRegistry;
import org.apache.omid.timestamp.storage.TimestampStorage;
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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

public class TestPanicker {

    private static final Logger LOG = LoggerFactory.getLogger(TestPanicker.class);

    @Mock
    private CommitTable.Writer mockWriter;
    @Mock
    private MetricsRegistry metrics;

    @BeforeMethod
    public void initMocksAndComponents() {
        MockitoAnnotations.initMocks(this);
    }

    @AfterMethod
    void afterMethod() {
        Mockito.reset(mockWriter);
    }

    // Note this test has been moved and refactored to TestTimestampOracle because
    // it tests the behaviour of the TimestampOracle.
    // Please, remove me in a future commit
    @Test(timeOut = 10_000)
    public void testTimestampOraclePanic() throws Exception {

        TimestampStorage storage = spy(new TimestampOracleImpl.InMemoryTimestampStorage());
        Panicker panicker = spy(new MockPanicker());

        doThrow(new RuntimeException("Out of memory")).when(storage).updateMaxTimestamp(anyLong(), anyLong());

        final TimestampOracleImpl tso = new TimestampOracleImpl(metrics, storage, panicker);
        tso.initialize();
        Thread allocThread = new Thread("AllocThread") {
            @Override
            public void run() {
                while (true) {
                    tso.next();
                }
            }
        };
        allocThread.start();

        verify(panicker, timeout(1000).atLeastOnce()).panic(anyString(), any(Throwable.class));

    }

    // Note this test has been moved and refactored to TestPersistenceProcessor because
    // it tests the behaviour of the PersistenceProcessor.
    // Please, remove me in a future commit
    @Test(timeOut = 10_000)
    public void testCommitTablePanic() throws Exception {

        Panicker panicker = spy(new MockPanicker());

        doThrow(new IOException("Unable to write@TestPanicker")).when(mockWriter).flush();

        final CommitTable.Client mockClient = mock(CommitTable.Client.class);
        CommitTable commitTable = new CommitTable() {
            @Override
            public Writer getWriter() {
                return mockWriter;
            }

            @Override
            public Client getClient() {
                return mockClient;
            }
        };

        LeaseManager leaseManager = mock(LeaseManager.class);
        doReturn(true).when(leaseManager).stillInLeasePeriod();
        TSOServerConfig config = new TSOServerConfig();
        ObjectPool<Batch> batchPool = new BatchPoolModule(config).getBatchPool();

        PersistenceProcessorHandler[] handlers = new PersistenceProcessorHandler[config.getNumConcurrentCTWriters()];
        for (int i = 0; i < config.getNumConcurrentCTWriters(); i++) {
            handlers[i] = new PersistenceProcessorHandler(metrics,
                                                          "localhost:1234",
                                                          leaseManager,
                                                          commitTable,
                                                          mock(ReplyProcessor.class),
                                                          mock(RetryProcessor.class),
                                                          panicker);
        }

        PersistenceProcessor proc = new PersistenceProcessorImpl(config,
                                                                 commitTable,
                                                                 batchPool,
                                                                 panicker,
                                                                 handlers,
                                                                 metrics);

        proc.addCommitToBatch(1, 2, null, new MonitoringContext(metrics));

        new RequestProcessorImpl(metrics, mock(TimestampOracle.class), proc, panicker, mock(TSOServerConfig.class));

        verify(panicker, timeout(1000).atLeastOnce()).panic(anyString(), any(Throwable.class));

    }

    // Note this test has been moved and refactored to TestPersistenceProcessor because
    // it tests the behaviour of the PersistenceProcessor.
    // Please, remove me in a future commit
    @Test(timeOut = 10_000)
    public void testRuntimeExceptionTakesDownDaemon() throws Exception {

        Panicker panicker = spy(new MockPanicker());

        final CommitTable.Writer mockWriter = mock(CommitTable.Writer.class);
        doThrow(new RuntimeException("Kaboom!")).when(mockWriter).addCommittedTransaction(anyLong(), anyLong());

        final CommitTable.Client mockClient = mock(CommitTable.Client.class);
        CommitTable commitTable = new CommitTable() {
            @Override
            public Writer getWriter() {
                return mockWriter;
            }

            @Override
            public Client getClient() {
                return mockClient;
            }
        };
        TSOServerConfig config = new TSOServerConfig();
        ObjectPool<Batch> batchPool = new BatchPoolModule(config).getBatchPool();

        PersistenceProcessorHandler[] handlers = new PersistenceProcessorHandler[config.getNumConcurrentCTWriters()];
        for (int i = 0; i < config.getNumConcurrentCTWriters(); i++) {
            handlers[i] = new PersistenceProcessorHandler(metrics,
                                                          "localhost:1234",
                                                          mock(LeaseManager.class),
                                                          commitTable,
                                                          mock(ReplyProcessor.class),
                                                          mock(RetryProcessor.class),
                                                          panicker);
        }

        PersistenceProcessor proc = new PersistenceProcessorImpl(config,
                                                                 commitTable,
                                                                 batchPool,
                                                                 panicker,
                                                                 handlers,
                                                                 metrics);
        proc.addCommitToBatch(1, 2, null, new MonitoringContext(metrics));

        new RequestProcessorImpl(metrics, mock(TimestampOracle.class), proc, panicker, mock(TSOServerConfig.class));

        verify(panicker, timeout(1000).atLeastOnce()).panic(anyString(), any(Throwable.class));

    }

}
