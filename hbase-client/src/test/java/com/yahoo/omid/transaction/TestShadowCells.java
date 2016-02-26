package com.yahoo.omid.transaction;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.yahoo.omid.committable.CommitTable;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestContext;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.yahoo.omid.transaction.CellUtils.hasCell;
import static com.yahoo.omid.transaction.CellUtils.hasShadowCell;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(groups = "sharedHBase")
public class TestShadowCells extends OmidTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(TestShadowCells.class);

    private static final String TSO_SERVER_HOST = "localhost";
    private static final int TSO_SERVER_PORT = 1234;

    private static final String TEST_TABLE = "test";
    private static final String TEST_FAMILY = "data";

    static final byte[] row = Bytes.toBytes("test-sc");
    private static final byte[] row1 = Bytes.toBytes("test-sc1");
    private static final byte[] row2 = Bytes.toBytes("test-sc2");
    private static final byte[] row3 = Bytes.toBytes("test-sc3");
    static final byte[] family = Bytes.toBytes(TEST_FAMILY);
    private static final byte[] qualifier = Bytes.toBytes("testdata");
    private static final byte[] data1 = Bytes.toBytes("testWrite-1");


    @Test(timeOut = 60_000)
    public void testShadowCellsBasics(ITestContext context) throws Exception {

        TransactionManager tm = newTransactionManager(context);

        TTable table = new TTable(hbaseConf, TEST_TABLE);

        HBaseTransaction t1 = (HBaseTransaction) tm.begin();

        // Test shadow cells are created properly
        Put put = new Put(row);
        put.add(family, qualifier, data1);
        table.put(t1, put);

        // Before commit test that only the cell is there
        assertTrue(hasCell(row, family, qualifier, t1.getStartTimestamp(), new TTableCellGetterAdapter(table)),
                "Cell should be there");
        assertFalse(hasShadowCell(row, family, qualifier, t1.getStartTimestamp(), new TTableCellGetterAdapter(table)),
                "Shadow cell shouldn't be there");

        tm.commit(t1);

        // After commit test that both cell and shadow cell are there
        assertTrue(hasCell(row, family, qualifier, t1.getStartTimestamp(), new TTableCellGetterAdapter(table)),
                "Cell should be there");
        assertTrue(hasShadowCell(row, family, qualifier, t1.getStartTimestamp(), new TTableCellGetterAdapter(table)),
                "Shadow cell should be there");

        // Test that we can make a valid read after adding a shadow cell without hitting the commit table
        CommitTable.Client commitTableClient = spy(getCommitTable(context).getClient().get());

        HBaseOmidClientConfiguration hbaseOmidClientConf = HBaseOmidClientConfiguration.create();
        hbaseOmidClientConf.setConnectionString(TSO_SERVER_HOST + ":" + TSO_SERVER_PORT);
        hbaseOmidClientConf.setHBaseConfiguration(hbaseConf);
        TransactionManager tm2 = HBaseTransactionManager.builder(hbaseOmidClientConf)
                                                        .commitTableClient(commitTableClient)
                                                        .build();

        Transaction t2 = tm2.begin();
        Get get = new Get(row);
        get.addColumn(family, qualifier);

        Result getResult = table.get(t2, get);
        assertTrue(Arrays.equals(data1, getResult.getValue(family, qualifier)), "Values should be the same");
        verify(commitTableClient, never()).getCommitTimestamp(anyLong());
    }

    @Test(timeOut = 60_000)
    public void testCrashingAfterCommitDoesNotWriteShadowCells(ITestContext context) throws Exception {
        CommitTable.Client commitTableClient = spy(getCommitTable(context).getClient().get());

        HBaseOmidClientConfiguration hbaseOmidClientConf = HBaseOmidClientConfiguration.create();
        hbaseOmidClientConf.setConnectionString(TSO_SERVER_HOST + ":" + TSO_SERVER_PORT);
        hbaseOmidClientConf.setHBaseConfiguration(hbaseConf);
        AbstractTransactionManager tm = spy((AbstractTransactionManager) HBaseTransactionManager.builder(hbaseOmidClientConf)
                .commitTableClient(commitTableClient)
                .build());
        // The following line emulates a crash after commit that is observed in (*) below
        doThrow(new RuntimeException()).when(tm).updateShadowCells(any(HBaseTransaction.class));

        TTable table = new TTable(hbaseConf, TEST_TABLE);

        HBaseTransaction t1 = (HBaseTransaction) tm.begin();

        // Test shadow cell are created properly
        Put put = new Put(row);
        put.add(family, qualifier, data1);
        table.put(t1, put);
        try {
            tm.commit(t1);
        } catch (Exception e) { // (*) crash
            // Do nothing
        }

        // After commit with the emulated crash, test that only the cell is there
        assertTrue(hasCell(row, family, qualifier, t1.getStartTimestamp(), new TTableCellGetterAdapter(table)),
                "Cell should be there");
        assertFalse(hasShadowCell(row, family, qualifier, t1.getStartTimestamp(), new TTableCellGetterAdapter(table)),
                "Shadow cell should not be there");

        Transaction t2 = tm.begin();
        Get get = new Get(row);
        get.addColumn(family, qualifier);

        Result getResult = table.get(t2, get);
        assertTrue(Arrays.equals(data1, getResult.getValue(family, qualifier)), "Shadow cell should not be there");
        verify(commitTableClient, times(1)).getCommitTimestamp(anyLong());
    }

    @Test(timeOut = 60_000)
    public void testShadowCellIsHealedAfterCommitCrash(ITestContext context) throws Exception {
        CommitTable.Client commitTableClient = spy(getCommitTable(context).getClient().get());

        HBaseOmidClientConfiguration hbaseOmidClientConf = HBaseOmidClientConfiguration.create();
        hbaseOmidClientConf.setConnectionString(TSO_SERVER_HOST + ":" + TSO_SERVER_PORT);
        hbaseOmidClientConf.setHBaseConfiguration(hbaseConf);
        AbstractTransactionManager tm = spy((AbstractTransactionManager) HBaseTransactionManager.builder(hbaseOmidClientConf)
                .commitTableClient(commitTableClient)
                .build());
        // The following line emulates a crash after commit that is observed in (*) below
        doThrow(new RuntimeException()).when(tm).updateShadowCells(any(HBaseTransaction.class));

        TTable table = new TTable(hbaseConf, TEST_TABLE);

        HBaseTransaction t1 = (HBaseTransaction) tm.begin();

        // Test shadow cell are created properly
        Put put = new Put(row);
        put.add(family, qualifier, data1);
        table.put(t1, put);
        try {
            tm.commit(t1);
        } catch (Exception e) { // (*) Crash
            // Do nothing
        }

        assertTrue(hasCell(row, family, qualifier, t1.getStartTimestamp(), new TTableCellGetterAdapter(table)),
                "Cell should be there");
        assertFalse(hasShadowCell(row, family, qualifier, t1.getStartTimestamp(), new TTableCellGetterAdapter(table)),
                "Shadow cell should not be there");

        Transaction t2 = tm.begin();
        Get get = new Get(row);
        get.addColumn(family, qualifier);

        // This get should heal the shadow cell
        Result getResult = table.get(t2, get);
        assertTrue(Arrays.equals(data1, getResult.getValue(family, qualifier)), "Values should be the same");
        verify(commitTableClient, times(1)).getCommitTimestamp(anyLong());

        assertTrue(hasCell(row, family, qualifier, t1.getStartTimestamp(), new TTableCellGetterAdapter(table)),
                "Cell should be there");
        assertTrue(hasShadowCell(row, family, qualifier, t1.getStartTimestamp(), new TTableCellGetterAdapter(table)),
                "Shadow cell should be there after being healed");

        // As the shadow cell is healed, this get shouldn't have to hit the storage,
        // so the number of invocations to commitTableClient.getCommitTimestamp()
        // should remain the same
        getResult = table.get(t2, get);
        assertTrue(Arrays.equals(data1, getResult.getValue(family, qualifier)), "Values should be the same");
        verify(commitTableClient, times(1)).getCommitTimestamp(anyLong());
    }

    @Test(timeOut = 60_000)
    public void testTransactionNeverCompletesWhenCommitThrowsAnInternalTransactionManagerExceptionUpdatingShadowCells(
            ITestContext context)
            throws Exception {
        CommitTable.Client commitTableClient = spy(getCommitTable(context).getClient().get());

        HBaseOmidClientConfiguration hbaseOmidClientConf = HBaseOmidClientConfiguration.create();
        hbaseOmidClientConf.setConnectionString(TSO_SERVER_HOST + ":" + TSO_SERVER_PORT);
        hbaseOmidClientConf.setHBaseConfiguration(hbaseConf);
        AbstractTransactionManager tm = spy((AbstractTransactionManager) HBaseTransactionManager.builder(hbaseOmidClientConf)
                .commitTableClient(commitTableClient)
                .build());

        final TTable table = new TTable(hbaseConf, TEST_TABLE);

        HBaseTransaction tx = (HBaseTransaction) tm.begin();

        Put put = new Put(row);
        put.add(family, qualifier, data1);
        table.put(tx, put);

        // This line emulates an error accessing the target table by disabling it
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                table.flushCommits();
                HBaseAdmin admin = hBaseUtils.getHBaseAdmin();
                admin.disableTable(table.getTableName());
                invocation.callRealMethod();
                return null;
            }
        }).when(tm).updateShadowCells(any(HBaseTransaction.class));

        // When committing, a TransactionManagerException should be thrown by tm.updateShadowCells(). The exception
        // must be catch internally by tm.commit(), avoiding the transaction completion on the commit table.
        // After that, a TransacionException is thrown to the application.
        // This requires to set the HConstants.HBASE_CLIENT_RETRIES_NUMBER in the HBase config to a finite number:
        // e.g -> hbaseConf.setInt(HBASE_CLIENT_RETRIES_NUMBER, 3); Otherwise it will get stuck in tm.commit();
        try {
            tm.commit(tx);
        } catch (TransactionException e) {
            // Expected, see comment above
        }

        // Re-enable table to allow the required checks below
        HBaseAdmin admin = hBaseUtils.getHBaseAdmin();
        admin.enableTable(table.getTableName());

        // 1) check that shadow cell is not created...
        assertTrue(hasCell(row, family, qualifier, tx.getStartTimestamp(), new TTableCellGetterAdapter(table)),
                "Cell should be there");
        assertFalse(hasShadowCell(row, family, qualifier, tx.getStartTimestamp(), new TTableCellGetterAdapter(table)),
                "Shadow cell should not be there");
        // 2) and thus, completeTransaction() was never called on the commit table...
        verify(commitTableClient, times(0)).completeTransaction(anyLong());
        // 3) ...and commit value still in commit table
        assertTrue(commitTableClient.getCommitTimestamp(tx.getStartTimestamp()).get().isPresent());

    }

    @Test(timeOut = 60_000)
    public void testRaceConditionBetweenReaderAndWriterThreads(final ITestContext context) throws Exception {
        final CountDownLatch readAfterCommit = new CountDownLatch(1);
        final CountDownLatch postCommitBegin = new CountDownLatch(1);
        final CountDownLatch postCommitEnd = new CountDownLatch(1);

        final AtomicBoolean readFailed = new AtomicBoolean(false);
        AbstractTransactionManager tm = spy((AbstractTransactionManager) newTransactionManager(context));

        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                LOG.info("Releasing readAfterCommit barrier");
                readAfterCommit.countDown();
                LOG.info("Waiting postCommitBegin barrier");
                postCommitBegin.await();
                invocation.callRealMethod();
                LOG.info("Releasing postCommitEnd barrier");
                postCommitEnd.countDown();
                return null;
            }
        }).when(tm).updateShadowCells(any(HBaseTransaction.class));

        // Start transaction on write thread
        TTable table = new TTable(hbaseConf, TEST_TABLE);

        final HBaseTransaction t1 = (HBaseTransaction) tm.begin();

        // Start read thread
        Thread readThread = new Thread("Read Thread") {
            @Override
            public void run() {
                LOG.info("Waiting readAfterCommit barrier");
                try {
                    readAfterCommit.await();
                    final TTable table = spy(new TTable(hbaseConf, TEST_TABLE));
                    doAnswer(new Answer<List<KeyValue>>() {
                        @SuppressWarnings("unchecked")
                        @Override
                        public List<KeyValue> answer(InvocationOnMock invocation) throws Throwable {
                            LOG.info("Release postCommitBegin barrier");
                            postCommitBegin.countDown();
                            LOG.info("Waiting postCommitEnd barrier");
                            postCommitEnd.await();
                            return (List<KeyValue>) invocation.callRealMethod();
                        }
                    }).when(table).filterCellsForSnapshot(Matchers.<List<Cell>>any(),
                            any(HBaseTransaction.class), anyInt());

                    TransactionManager tm = newTransactionManager(context);
                    if (hasShadowCell(row,
                            family,
                            qualifier,
                            t1.getStartTimestamp(),
                            new TTableCellGetterAdapter(table))) {
                        readFailed.set(true);
                    }

                    Transaction t = tm.begin();
                    Get get = new Get(row);
                    get.addColumn(family, qualifier);

                    Result getResult = table.get(t, get);
                    Cell cell = getResult.getColumnLatestCell(family, qualifier);
                    if (!Arrays.equals(data1, CellUtil.cloneValue(cell))
                            || !hasShadowCell(row,
                            family,
                            qualifier,
                            cell.getTimestamp(),
                            new TTableCellGetterAdapter(table))) {
                        readFailed.set(true);
                    } else {
                        LOG.info("Read succeeded");
                    }
                } catch (Throwable e) {
                    readFailed.set(true);
                    LOG.error("Error whilst reading", e);
                }
            }
        };
        readThread.start();

        // Write data
        Put put = new Put(row);
        put.add(family, qualifier, data1);
        table.put(t1, put);
        tm.commit(t1);

        readThread.join();

        assertFalse(readFailed.get(), "Read should have succeeded");

    }

    // TODO: After removing the legacy shadow cell suffix, maybe we should mix the assertions in this test with
    // the ones in the previous tests in a further commit

    /**
     * Test that the new client can read shadow cells written by the old client.
     */
    @Test
    public void testGetOldShadowCells(ITestContext context) throws Exception {

        TransactionManager tm = newTransactionManager(context);

        TTable table = new TTable(hbaseConf, TEST_TABLE);
        HTableInterface htable = table.getHTable();

        // Test shadow cell are created properly
        HBaseTransaction t1 = (HBaseTransaction) tm.begin();
        Put put = new Put(row1);
        put.add(family, qualifier, data1);
        table.put(t1, put);
        tm.commit(t1);

        HBaseTransaction t2 = (HBaseTransaction) tm.begin();
        put = new Put(row2);
        put.add(family, qualifier, data1);
        table.put(t2, put);
        tm.commit(t2);

        HBaseTransaction t3 = (HBaseTransaction) tm.begin();
        put = new Put(row3);
        put.add(family, qualifier, data1);
        table.put(t3, put);
        tm.commit(t3);

        // ensure that transaction is no longer in commit table
        // the only place that should have the mapping is the shadow cells
        CommitTable.Client commitTableClient = spy(getCommitTable(context).getClient().get());
        Optional<CommitTable.CommitTimestamp> ct1 = commitTableClient.getCommitTimestamp(t1.getStartTimestamp()).get();
        Optional<CommitTable.CommitTimestamp> ct2 = commitTableClient.getCommitTimestamp(t2.getStartTimestamp()).get();
        Optional<CommitTable.CommitTimestamp> ct3 = commitTableClient.getCommitTimestamp(t3.getStartTimestamp()).get();
        assertFalse(ct1.isPresent(), "Shouldn't exist in commit table");
        assertFalse(ct2.isPresent(), "Shouldn't exist in commit table");
        assertFalse(ct3.isPresent(), "Shouldn't exist in commit table");

        // delete new shadow cell
        Delete del = new Delete(row2);
        del.deleteColumn(family, CellUtils.addShadowCellSuffix(qualifier));
        htable.delete(del);
        htable.flushCommits();

        // verify that we can't read now (since shadow cell is missing)
        Transaction t4 = tm.begin();
        Get get = new Get(row2);
        get.addColumn(family, qualifier);

        Result getResult = table.get(t4, get);
        assertTrue(getResult.isEmpty(), "Should have nothing");

        Transaction t5 = tm.begin();
        Scan s = new Scan();
        ResultScanner scanner = table.getScanner(t5, s);
        Result result1 = scanner.next();
        Result result2 = scanner.next();
        Result result3 = scanner.next();
        scanner.close();

        assertNull(result3);
        assertTrue(Arrays.equals(result1.getRow(), row1), "Should have first row");
        assertTrue(Arrays.equals(result2.getRow(), row3), "Should have third row");
        assertTrue(result1.containsColumn(family, qualifier), "Should have column family");
        assertTrue(result2.containsColumn(family, qualifier), "Should have column family");

        // now add in the previous legacy shadow cell for that row
        put = new Put(row2);
        put.add(family,
                addLegacyShadowCellSuffix(qualifier),
                t2.getStartTimestamp(),
                Bytes.toBytes(t2.getCommitTimestamp()));
        htable.put(put);

        // we should NOT be able to read that row now, even though
        // it has a legacy shadow cell
        Transaction t6 = tm.begin();
        get = new Get(row2);
        get.addColumn(family, qualifier);

        getResult = table.get(t6, get);
        assertFalse(getResult.containsColumn(family, qualifier), "Should NOT have column");

        Transaction t7 = tm.begin();
        s = new Scan();
        scanner = table.getScanner(t7, s);
        result1 = scanner.next();
        result2 = scanner.next();
        result3 = scanner.next();
        scanner.close();

        assertNull(result3, "There should only be 2 rows");
        assertTrue(Arrays.equals(result1.getRow(), row1), "Should have first row");
        assertTrue(Arrays.equals(result2.getRow(), row3), "Should have third row");
        assertTrue(result1.containsColumn(family, qualifier), "Should have column family");
        assertTrue(result2.containsColumn(family, qualifier), "Should have column family");
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Helper methods
    // ----------------------------------------------------------------------------------------------------------------

    private static final byte[] LEGACY_SHADOW_CELL_SUFFIX = ":OMID_CTS".getBytes(Charsets.UTF_8);

    private static byte[] addLegacyShadowCellSuffix(byte[] qualifier) {
        return com.google.common.primitives.Bytes.concat(qualifier, LEGACY_SHADOW_CELL_SUFFIX);
    }

}
