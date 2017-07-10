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

import com.google.common.collect.Sets;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.omid.TestUtils;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.tso.TSOMockModule;
import org.apache.omid.tso.TSOServer;
import org.apache.omid.tso.TSOServerConfig;
import org.apache.omid.tso.util.DummyCellIdImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestIntegrationOfTSOClientServerBasicFunctionality {

    private static final Logger LOG = LoggerFactory.getLogger(TestIntegrationOfTSOClientServerBasicFunctionality.class);

    private static final String TSO_SERVER_HOST = "localhost";
    private int tsoServerPortForTest;

    // Cells for tests
    private final static CellId c1 = new DummyCellIdImpl(0xdeadbeefL);
    private final static CellId c2 = new DummyCellIdImpl(0xfeedcafeL);

    // Required infrastructure for TSO tsoClient-server integration testing
    private TSOServer tsoServer;
    private TSOClient tsoClient;
    private TSOClient justAnotherTSOClient;
    private CommitTable.Client commitTableClient;

    @BeforeClass
    public void setup() throws Exception {

        tsoServerPortForTest = TestUtils.getFreeLocalPort();

        TSOServerConfig tsoConfig = new TSOServerConfig();
        tsoConfig.setConflictMapSize(1000);
        tsoConfig.setPort(tsoServerPortForTest);
        Module tsoServerMockModule = new TSOMockModule(tsoConfig);
        Injector injector = Guice.createInjector(tsoServerMockModule);

        CommitTable commitTable = injector.getInstance(CommitTable.class);
        commitTableClient = commitTable.getClient();

        LOG.info("==================================================================================================");
        LOG.info("======================================= Init TSO Server ==========================================");
        LOG.info("==================================================================================================");

        tsoServer = injector.getInstance(TSOServer.class);
        tsoServer.startAndWait();
        TestUtils.waitForSocketListening(TSO_SERVER_HOST, tsoServerPortForTest, 100);

        LOG.info("==================================================================================================");
        LOG.info("===================================== TSO Server Initialized =====================================");
        LOG.info("==================================================================================================");

        LOG.info("==================================================================================================");
        LOG.info("======================================= Setup TSO Clients ========================================");
        LOG.info("==================================================================================================");

        // Configure direct connection to the server
        OmidClientConfiguration tsoClientConf = new OmidClientConfiguration();
        tsoClientConf.setConnectionString(TSO_SERVER_HOST + ":" + tsoServerPortForTest);

        tsoClient = TSOClient.newInstance(tsoClientConf);
        justAnotherTSOClient = TSOClient.newInstance(tsoClientConf);

        LOG.info("==================================================================================================");
        LOG.info("===================================== TSO Clients Initialized ====================================");
        LOG.info("==================================================================================================");

        Thread.currentThread().setName("Test Thread");

    }

    @AfterClass
    public void tearDown() throws Exception {

        tsoClient.close().get();

        tsoServer.stopAndWait();
        tsoServer = null;
        TestUtils.waitForSocketNotListening(TSO_SERVER_HOST, tsoServerPortForTest, 1000);

    }

    @Test(timeOut = 30_000)
    public void testTimestampsOrderingGrowMonotonically() throws Exception {
        long referenceTimestamp;
        long startTsTx1 = tsoClient.getNewStartTimestamp().get();
        referenceTimestamp = startTsTx1;

        long startTsTx2 = tsoClient.getNewStartTimestamp().get();
        assertEquals(startTsTx2, ++referenceTimestamp, "Should grow monotonically");
        assertTrue(startTsTx2 > startTsTx1, "Two timestamps obtained consecutively should grow");

        long commitTsTx2 = tsoClient.commit(startTsTx2, Sets.newHashSet(c1)).get();
        assertEquals(commitTsTx2, ++referenceTimestamp, "Should grow monotonically");

        long commitTsTx1 = tsoClient.commit(startTsTx1, Sets.newHashSet(c2)).get();
        assertEquals(commitTsTx1, ++referenceTimestamp, "Should grow monotonically");

        long startTsTx3 = tsoClient.getNewStartTimestamp().get();
        assertEquals(startTsTx3, ++referenceTimestamp, "Should grow monotonically");
    }

    @Test(timeOut = 30_000)
    public void testSimpleTransactionWithNoWriteSetCanCommit() throws Exception {
        long startTsTx1 = tsoClient.getNewStartTimestamp().get();
        long commitTsTx1 = tsoClient.commit(startTsTx1, Sets.<CellId>newHashSet()).get();
        assertTrue(commitTsTx1 > startTsTx1);
    }

    @Test(timeOut = 30_000)
    public void testTransactionWithMassiveWriteSetCanCommit() throws Exception {
        long startTs = tsoClient.getNewStartTimestamp().get();

        Set<CellId> cells = new HashSet<>();
        for (int i = 0; i < 1_000_000; i++) {
            cells.add(new DummyCellIdImpl(i));
        }

        long commitTs = tsoClient.commit(startTs, cells).get();
        assertTrue(commitTs > startTs, "Commit TS should be higher than Start TS");
    }

    @Test(timeOut = 30_000)
    public void testMultipleSerialCommitsDoNotConflict() throws Exception {
        long startTsTx1 = tsoClient.getNewStartTimestamp().get();
        long commitTsTx1 = tsoClient.commit(startTsTx1, Sets.newHashSet(c1)).get();
        assertTrue(commitTsTx1 > startTsTx1, "Commit TS must be greater than Start TS");

        long startTsTx2 = tsoClient.getNewStartTimestamp().get();
        assertTrue(startTsTx2 > commitTsTx1, "TS should grow monotonically");

        long commitTsTx2 = tsoClient.commit(startTsTx2, Sets.newHashSet(c1, c2)).get();
        assertTrue(commitTsTx2 > startTsTx2, "Commit TS must be greater than Start TS");

        long startTsTx3 = tsoClient.getNewStartTimestamp().get();
        long commitTsTx3 = tsoClient.commit(startTsTx3, Sets.newHashSet(c2)).get();
        assertTrue(commitTsTx3 > startTsTx3, "Commit TS must be greater than Start TS");
    }

    @Test(timeOut = 30_000)
    public void testCommitWritesToCommitTable() throws Exception {
        long startTsForTx1 = tsoClient.getNewStartTimestamp().get();
        long startTsForTx2 = tsoClient.getNewStartTimestamp().get();
        assertTrue(startTsForTx2 > startTsForTx1, "Start TS should grow");

        assertFalse(commitTableClient.getCommitTimestamp(startTsForTx1).get().isPresent(),
                "Commit TS for Tx1 shouldn't appear in Commit Table");

        long commitTsForTx1 = tsoClient.commit(startTsForTx1, Sets.newHashSet(c1)).get();
        assertTrue(commitTsForTx1 > startTsForTx1, "Commit TS should be higher than Start TS for the same tx");

        Long commitTs1InCommitTable = commitTableClient.getCommitTimestamp(startTsForTx1).get().get().getValue();
        assertNotNull(commitTs1InCommitTable, "Tx is committed, should return as such from Commit Table");
        assertEquals(commitTsForTx1, (long) commitTs1InCommitTable,
                "getCommitTimestamp() & commit() should report same Commit TS value for same tx");
        assertTrue(commitTs1InCommitTable > startTsForTx2, "Commit TS should be higher than tx's Start TS");
    }

    @Test(timeOut = 30_000)
    public void testTwoConcurrentTxWithOverlappingWritesetsHaveConflicts() throws Exception {
        long startTsTx1 = tsoClient.getNewStartTimestamp().get();
        long startTsTx2 = tsoClient.getNewStartTimestamp().get();
        assertTrue(startTsTx2 > startTsTx1, "Second TX should have higher TS");

        long commitTsTx1 = tsoClient.commit(startTsTx1, Sets.newHashSet(c1)).get();
        assertTrue(commitTsTx1 > startTsTx1, "Commit TS must be higher than Start TS for the same tx");

        try {
            tsoClient.commit(startTsTx2, Sets.newHashSet(c1, c2)).get();
            Assert.fail("Second TX should fail on commit");
        } catch (ExecutionException ee) {
            assertEquals(AbortException.class, ee.getCause().getClass(), "Should have aborted");
        }
    }

    @Test(timeOut = 30_000)
    public void testTransactionStartedBeforeFenceAborts() throws Exception {

        long startTsTx1 = tsoClient.getNewStartTimestamp().get();

        long fenceID = tsoClient.getFence(c1.getTableId()).get();

        assertTrue(fenceID > startTsTx1, "Fence ID should be higher thank Tx1ID");

        try {
            tsoClient.commit(startTsTx1, Sets.newHashSet(c1, c2)).get();
            Assert.fail("TX should fail on commit");
        } catch (ExecutionException ee) {
            assertEquals(AbortException.class, ee.getCause().getClass(), "Should have aborted");
        }
    }

    @Test(timeOut = 30_000)
    public void testTransactionStartedBeforeNonOverlapFenceCommits() throws Exception {

        long startTsTx1 = tsoClient.getNewStartTimestamp().get();

        tsoClient.getFence(7).get();

        try {
            tsoClient.commit(startTsTx1, Sets.newHashSet(c1, c2)).get();
        } catch (ExecutionException ee) {
            Assert.fail("TX should successfully commit");        }
    }

    @Test(timeOut = 30_000)
    public void testTransactionStartedAfterFenceCommits() throws Exception {

        tsoClient.getFence(c1.getTableId()).get();

        long startTsTx1 = tsoClient.getNewStartTimestamp().get();

        try {
            tsoClient.commit(startTsTx1, Sets.newHashSet(c1, c2)).get();
        } catch (ExecutionException ee) {
            Assert.fail("TX should successfully commit");
        }
    }

    @Test(timeOut = 30_000)
    public void testConflictsAndMonotonicallyTimestampGrowthWithTwoDifferentTSOClients() throws Exception {
        long startTsTx1Client1 = tsoClient.getNewStartTimestamp().get();
        long startTsTx2Client1 = tsoClient.getNewStartTimestamp().get();
        long startTsTx3Client1 = tsoClient.getNewStartTimestamp().get();

        tsoClient.commit(startTsTx1Client1, Sets.newHashSet(c1)).get();
        try {
            tsoClient.commit(startTsTx3Client1, Sets.newHashSet(c1, c2)).get();
            Assert.fail("Second commit should fail as conflicts with the previous concurrent one");
        } catch (ExecutionException ee) {
            assertEquals(AbortException.class, ee.getCause().getClass(), "Should have aborted");
        }
        long startTsTx4Client2 = justAnotherTSOClient.getNewStartTimestamp().get();

        assertFalse(commitTableClient.getCommitTimestamp(startTsTx3Client1).get().isPresent(), "Tx3 didn't commit");
        long commitTSTx1 = commitTableClient.getCommitTimestamp(startTsTx1Client1).get().get().getValue();
        assertTrue(commitTSTx1 > startTsTx2Client1, "Tx1 committed after Tx2 started");
        assertTrue(commitTSTx1 < startTsTx4Client2, "Tx1 committed before Tx4 started on the other TSO Client");
    }

}
