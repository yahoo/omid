package com.yahoo.omid.transaction;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.SettableFuture;
import com.yahoo.omid.TestUtils;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.CommitTable.CommitTimestamp;
import com.yahoo.omid.committable.InMemoryCommitTable;
import com.yahoo.omid.transaction.Transaction.Status;
import com.yahoo.omid.tso.ProgrammableTSOServer;
import com.yahoo.omid.tsoclient.TSOClient;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static com.yahoo.omid.committable.CommitTable.CommitTimestamp.Location.COMMIT_TABLE;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(groups = "sharedHBase")
public class TestTxMgrFailover extends OmidTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(TestTxMgrFailover.class);

    private static final int TSO_SERVER_PORT = 3333;
    private static final String TSO_SERVER_HOST = "localhost";

    private static final long TX1_ST = 1L;
    private static final long TX1_CT = 2L;

    private static final byte[] qualifier = Bytes.toBytes("test-qual");
    private static final byte[] row1 = Bytes.toBytes("row1");
    private static final byte[] data1 = Bytes.toBytes("testWrite-1");

    // Used in test assertions
    private InMemoryCommitTable commitTable;

    private CommitTable.Client commitTableClient;
    private CommitTable.Writer commitTableWriter;

    // Allows to prepare the required responses to client request operations
    private ProgrammableTSOServer tso;

    // The transaction manager under test
    private HBaseTransactionManager tm;

    @BeforeClass(alwaysRun = true)
    public void beforeClass() throws Exception {
        // ------------------------------------------------------------------------------------------------------------
        // ProgrammableTSOServer  setup
        // ------------------------------------------------------------------------------------------------------------
        tso = new ProgrammableTSOServer(TSO_SERVER_PORT);
        TestUtils.waitForSocketListening(TSO_SERVER_HOST, TSO_SERVER_PORT, 100);
    }

    @BeforeMethod(alwaysRun = true, timeOut = 30_000)
    public void beforeMethod() throws ExecutionException, InterruptedException, OmidInstantiationException {

        commitTable = new InMemoryCommitTable(); // Use an in-memory commit table to speed up tests
        commitTableClient = spy(commitTable.getClient().get());
        commitTableWriter = spy(commitTable.getWriter().get());

        HBaseOmidClientConfiguration hbaseOmidClientConf = HBaseOmidClientConfiguration.create();
        hbaseOmidClientConf.setConnectionString(TSO_SERVER_HOST + ":" + TSO_SERVER_PORT);
        hbaseOmidClientConf.setHBaseConfiguration(hbaseConf);
        TSOClient tsoClientForTM = spy(TSOClient.newInstance(hbaseOmidClientConf.getOmidClientConfiguration()));

        tm = spy(HBaseTransactionManager.builder(hbaseOmidClientConf)
                .tsoClient(tsoClientForTM)
                .commitTableClient(commitTableClient)
                .build());
    }

    @Test
    public void testAbortResponseFromTSOThrowsRollbackExceptionInClient() throws Exception {
        // Program the TSO to return an ad-hoc Timestamp and an abort response for tx 1
        tso.queueResponse(new ProgrammableTSOServer.TimestampResponse(TX1_ST));
        tso.queueResponse(new ProgrammableTSOServer.AbortResponse(TX1_ST));

        try (TTable txTable = new TTable(hbaseConf, TEST_TABLE)) {
            HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
            assertEquals(tx1.getStartTimestamp(), TX1_ST);
            Put put = new Put(row1);
            put.add(TEST_FAMILY.getBytes(), qualifier, data1);
            txTable.put(tx1, put);
            assertEquals(hBaseUtils.countRows(new HTable(hbaseConf, TEST_TABLE)), 1, "Rows should be 1!");
            checkOperationSuccessOnCell(KeyValue.Type.Put, data1, TEST_TABLE.getBytes(), row1, TEST_FAMILY.getBytes(),
                    qualifier);

            try {
                tm.commit(tx1);
                fail();
            } catch (RollbackException e) {
                // Expected!

            }

            // Check transaction status
            assertEquals(tx1.getStatus(), Status.ROLLEDBACK);
            assertEquals(tx1.getCommitTimestamp(), 0);
            // Check the cleanup process did its job and the committed data is NOT there
            checkOperationSuccessOnCell(KeyValue.Type.Delete, null, TEST_TABLE.getBytes(), row1, TEST_FAMILY.getBytes(),
                    qualifier);
        }

    }

    @Test
    public void testClientReceivesSuccessfulCommitForNonInvalidatedTxCommittedByPreviousTSO()
            throws Exception {

        // Program the TSO to return an ad-hoc Timestamp and an commit response with heuristic actions
        tso.queueResponse(new ProgrammableTSOServer.TimestampResponse(TX1_ST));
        tso.queueResponse(new ProgrammableTSOServer.CommitResponse(true, TX1_ST, TX1_CT));
        // Simulate that tx1 was committed by writing to commit table
        commitTableWriter.addCommittedTransaction(TX1_ST, TX1_CT);
        commitTableWriter.flush();
        assertEquals(commitTable.countElements(), 1, "Rows should be 1!");

        try (TTable txTable = new TTable(hbaseConf, TEST_TABLE)) {
            HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
            assertEquals(tx1.getStartTimestamp(), TX1_ST);
            Put put = new Put(row1);
            put.add(TEST_FAMILY.getBytes(), qualifier, data1);
            txTable.put(tx1, put);
            // Should succeed
            tm.commit(tx1);

            // Check transaction status
            assertEquals(tx1.getStatus(), Status.COMMITTED);
            assertEquals(tx1.getCommitTimestamp(), TX1_CT);
            // Check the cleanup process did its job and the committed data is there
            // Note that now we do not clean up the commit table when exercising the heuristic actions
            assertEquals(commitTable.countElements(), 1,
                    "Rows should be 1! We don't have to clean CT in this case");
            Optional<CommitTimestamp>
                    optionalCT =
                    tm.commitTableClient.getCommitTimestamp(TX1_ST).get();
            assertTrue(optionalCT.isPresent());
            checkOperationSuccessOnCell(KeyValue.Type.Put, data1, TEST_TABLE.getBytes(), row1, TEST_FAMILY.getBytes(),
                    qualifier);
        }

    }

    @Test
    public void testClientReceivesRollbackExceptionForInvalidatedTxCommittedByPreviousTSO()
            throws Exception {

        // Program the TSO to return an ad-hoc Timestamp and a commit response with heuristic actions
        tso.queueResponse(new ProgrammableTSOServer.TimestampResponse(TX1_ST));
        tso.queueResponse(new ProgrammableTSOServer.CommitResponse(true, TX1_ST, TX1_CT));
        // Simulate that tx1 was committed by writing to commit table but was later invalidated
        commitTableClient.tryInvalidateTransaction(TX1_ST);
        assertEquals(commitTable.countElements(), 1, "Rows should be 1!");

        try (TTable txTable = new TTable(hbaseConf, TEST_TABLE)) {
            HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
            assertEquals(tx1.getStartTimestamp(), TX1_ST);
            Put put = new Put(row1);
            put.add(TEST_FAMILY.getBytes(), qualifier, data1);
            txTable.put(tx1, put);
            try {
                tm.commit(tx1);
                fail();
            } catch (RollbackException e) {
                // Exception
            }

            // Check transaction status
            assertEquals(tx1.getStatus(), Status.ROLLEDBACK);
            assertEquals(tx1.getCommitTimestamp(), 0);
            // Check the cleanup process did its job and the uncommitted data is NOT there
            assertEquals(commitTable.countElements(), 1, "Rows should be 1! Dirty data should be there");
            Optional<CommitTimestamp>
                    optionalCT =
                    tm.commitTableClient.getCommitTimestamp(TX1_ST).get();
            assertTrue(optionalCT.isPresent());
            assertFalse(optionalCT.get().isValid());
            checkOperationSuccessOnCell(KeyValue.Type.Delete, null, TEST_TABLE.getBytes(), row1, TEST_FAMILY.getBytes(),
                    qualifier);
        }

    }

    @Test
    public void testClientReceivesNotificationOfANewTSOCanInvalidateTransaction() throws Exception {

        // Program the TSO to return an ad-hoc Timestamp and a commit response with heuristic actions
        tso.queueResponse(new ProgrammableTSOServer.TimestampResponse(TX1_ST));
        tso.queueResponse(new ProgrammableTSOServer.CommitResponse(true, TX1_ST, TX1_CT));

        assertEquals(commitTable.countElements(), 0, "Rows should be 0!");

        try (TTable txTable = new TTable(hbaseConf, TEST_TABLE)) {
            HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
            assertEquals(tx1.getStartTimestamp(), TX1_ST);
            Put put = new Put(row1);
            put.add(TEST_FAMILY.getBytes(), qualifier, data1);
            txTable.put(tx1, put);
            try {
                tm.commit(tx1);
                fail();
            } catch (RollbackException e) {
                // Expected
            }

            // Check transaction status
            assertEquals(tx1.getStatus(), Status.ROLLEDBACK);
            assertEquals(tx1.getCommitTimestamp(), 0);
            // Check the cleanup process did its job and the transaction was invalidated
            // Uncommitted data should NOT be there
            assertEquals(commitTable.countElements(), 1, "Rows should be 1! Dirty data should be there");
            Optional<CommitTimestamp>
                    optionalCT =
                    tm.commitTableClient.getCommitTimestamp(TX1_ST).get();
            assertTrue(optionalCT.isPresent());
            assertFalse(optionalCT.get().isValid());
            checkOperationSuccessOnCell(KeyValue.Type.Delete, null, TEST_TABLE.getBytes(), row1, TEST_FAMILY.getBytes(),
                    qualifier);
        }

    }

    @Test
    public void testClientSuccessfullyCommitsWhenReceivingNotificationOfANewTSOAandCANTInvalidateTransaction()
            throws Exception {

        // Program the TSO to return an ad-hoc Timestamp and a commit response with heuristic actions
        tso.queueResponse(new ProgrammableTSOServer.TimestampResponse(TX1_ST));
        tso.queueResponse(new ProgrammableTSOServer.CommitResponse(true, TX1_ST, TX1_CT));

        // Simulate that the original TSO was able to add the tx to commit table in the meantime
        commitTableWriter.addCommittedTransaction(TX1_ST, TX1_CT);
        commitTableWriter.flush();
        assertEquals(commitTable.countElements(), 1, "Rows should be 1!");
        SettableFuture<Optional<CommitTimestamp>> f1 = SettableFuture.<Optional<CommitTimestamp>>create();
        f1.set(Optional.<CommitTimestamp>absent());
        SettableFuture<Optional<CommitTimestamp>> f2 = SettableFuture.<Optional<CommitTimestamp>>create();
        f2.set(Optional.of(new CommitTimestamp(COMMIT_TABLE, TX1_CT, true)));
        doReturn(f1).doReturn(f2).when(commitTableClient).getCommitTimestamp(TX1_ST);

        try (TTable txTable = new TTable(hbaseConf, TEST_TABLE)) {
            HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
            assertEquals(tx1.getStartTimestamp(), TX1_ST);
            Put put = new Put(row1);
            put.add(TEST_FAMILY.getBytes(), qualifier, data1);
            txTable.put(tx1, put);

            tm.commit(tx1);

            // Check transaction status
            assertEquals(tx1.getStatus(), Status.COMMITTED);
            assertEquals(tx1.getCommitTimestamp(), TX1_CT);
            // Check the cleanup process did its job and the committed data is there
            // Note that now we do not clean up the commit table when exercising the heuristic actions
            assertEquals(commitTable.countElements(), 1,
                    "Rows should be 1! We don't have to clean CT in this case");
            checkOperationSuccessOnCell(KeyValue.Type.Put, data1, TEST_TABLE.getBytes(), row1, TEST_FAMILY.getBytes(),
                    qualifier);
        }

    }

    @Test
    public void testClientReceivesATransactionExceptionWhenReceivingNotificationOfANewTSOAndCANTInvalidateTransactionAndCTCheckIsUnsuccessful()
            throws Exception {

        // Program the TSO to return an ad-hoc Timestamp and a commit response with heuristic actions
        tso.queueResponse(new ProgrammableTSOServer.TimestampResponse(TX1_ST));
        tso.queueResponse(new ProgrammableTSOServer.CommitResponse(true, TX1_ST, TX1_CT));

        // Simulate that the original TSO was able to add the tx to commit table in the meantime
        SettableFuture<Boolean> f = SettableFuture.create();
        f.set(false);
        doReturn(f).when(commitTableClient).tryInvalidateTransaction(TX1_ST);

        assertEquals(commitTable.countElements(), 0, "Rows should be 0!");

        try (TTable txTable = new TTable(hbaseConf, TEST_TABLE)) {
            HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
            assertEquals(tx1.getStartTimestamp(), TX1_ST);
            Put put = new Put(row1);
            put.add(TEST_FAMILY.getBytes(), qualifier, data1);
            txTable.put(tx1, put);
            try {
                tm.commit(tx1);
                fail();
            } catch (TransactionException e) {
                // Expected but is not good because we're not able to determine the tx outcome
            }

            // Check transaction status
            assertEquals(tx1.getStatus(), Status.RUNNING);
            assertEquals(tx1.getCommitTimestamp(), 0);
        }

    }

    // ----------------------------------------------------------------------------------------------------------------
    // Helper methods
    // ----------------------------------------------------------------------------------------------------------------

    protected void checkOperationSuccessOnCell(KeyValue.Type targetOp, @Nullable byte[] expectedValue,
                                               byte[] tableName,
                                               byte[] row,
                                               byte[] fam,
                                               byte[] col) {

        try (HTable table = new HTable(hbaseConf, tableName)) {
            Get get = new Get(row).setMaxVersions(1);
            Result result = table.get(get);
            Cell latestCell = result.getColumnLatestCell(fam, col);

            switch (targetOp) {
                case Put:
                    assertEquals(latestCell.getTypeByte(), targetOp.getCode());
                    assertEquals(CellUtil.cloneValue(latestCell), expectedValue);
                    LOG.trace("Value for " + Bytes.toString(tableName) + ":"
                            + Bytes.toString(row) + ":" + Bytes.toString(fam) + ":"
                            + Bytes.toString(col) + "=>" + Bytes.toString(CellUtil.cloneValue(latestCell))
                            + " (" + Bytes.toString(expectedValue) + " expected)");
                    break;
                case Delete:
                    LOG.trace("Value for " + Bytes.toString(tableName) + ":"
                            + Bytes.toString(row) + ":" + Bytes.toString(fam)
                            + Bytes.toString(col) + " deleted");
                    assertNull(latestCell);
                    break;
                default:
                    fail();
            }
        } catch (IOException e) {
            LOG.error("Error reading row " + Bytes.toString(tableName) + ":"
                    + Bytes.toString(row) + ":" + Bytes.toString(fam)
                    + Bytes.toString(col), e);
            fail();
        }
    }

}
