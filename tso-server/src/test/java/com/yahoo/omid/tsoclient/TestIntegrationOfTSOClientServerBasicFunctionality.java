package com.yahoo.omid.tsoclient;

import com.google.common.collect.Sets;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.yahoo.omid.TestUtils;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.tso.TSOMockModule;
import com.yahoo.omid.tso.TSOServer;
import com.yahoo.omid.tso.TSOServerCommandLineConfig;
import com.yahoo.omid.tso.util.DummyCellIdImpl;
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
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertNotNull;

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

        String[] configArgs = new String[]{"-port", Integer.toString(tsoServerPortForTest), "-maxItems", "1000"};
        TSOServerCommandLineConfig tsoConfig = TSOServerCommandLineConfig.parseConfig(configArgs);
        Module tsoServerMockModule = new TSOMockModule(tsoConfig);
        Injector injector = Guice.createInjector(tsoServerMockModule);

        CommitTable commitTable = injector.getInstance(CommitTable.class);
        commitTableClient = commitTable.getClient().get();

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
        TSOClientConfiguration omidConf = TSOClientConfiguration.builder()
                .connectionString(TSO_SERVER_HOST + ":" + tsoServerPortForTest)
                .build();
        tsoClient = TSOClient.builder(omidConf).build();
        justAnotherTSOClient = TSOClient.builder(omidConf).build();

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
        assertNotNull("Tx is committed, should return as such from Commit Table", commitTs1InCommitTable);
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
