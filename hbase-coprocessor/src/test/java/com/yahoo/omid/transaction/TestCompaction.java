package com.yahoo.omid.transaction;

import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.yahoo.omid.HBaseShims;
import com.yahoo.omid.TestUtils;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.tso.TSOServer;
import com.yahoo.omid.tso.TSOServerCommandLineConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.util.Bytes;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.yahoo.omid.committable.hbase.CommitTableConstants.COMMIT_TABLE_DEFAULT_NAME;
import static com.yahoo.omid.committable.hbase.CommitTableConstants.COMMIT_TABLE_FAMILY;
import static com.yahoo.omid.committable.hbase.CommitTableConstants.LOW_WATERMARK_FAMILY;
import static com.yahoo.omid.timestamp.storage.HBaseTimestampStorage.TIMESTAMP_TABLE_DEFAULT_NAME;
import static com.yahoo.omid.timestamp.storage.HBaseTimestampStorage.TSO_FAMILY;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

public class TestCompaction {

    private static final Logger LOG = LoggerFactory.getLogger(TestCompaction.class);

    private static final String TEST_FAMILY = "test-fam";
    private static final String TEST_QUALIFIER = "test-qual";

    private final byte[] fam = Bytes.toBytes(TEST_FAMILY);
    private final byte[] qual = Bytes.toBytes(TEST_QUALIFIER);
    private final byte[] data = Bytes.toBytes("testWrite-1");

    private static final int MAX_VERSIONS = 3;

    private Random randomGenerator;
    private AbstractTransactionManager tm;

    private Injector injector;

    private HBaseAdmin admin;
    private Configuration hbaseConf;
    private HBaseTestingUtility hbaseTestUtil;
    private MiniHBaseCluster hbaseCluster;

    private TSOServer tso;

    private AggregationClient aggregationClient;
    private CommitTable commitTable;

    @BeforeClass
    public void setupTestCompation() throws Exception {
        String[] configArgs = new String[]{"-port", Integer.toString(1234), "-maxItems", "1"};
        TSOServerCommandLineConfig tsoConfig = TSOServerCommandLineConfig.parseConfig(configArgs);
        injector = Guice.createInjector(new TSOForHBaseCompactorTestModule(tsoConfig));
        hbaseConf = injector.getInstance(Configuration.class);

        // settings required for #testDuplicateDeletes()
        hbaseConf.setInt("hbase.hstore.compaction.min", 2);
        hbaseConf.setInt("hbase.hstore.compaction.max", 2);

        setupHBase();
        aggregationClient = new AggregationClient(hbaseConf);
        admin = new HBaseAdmin(hbaseConf);
        createRequiredHBaseTables();
        setupTSO();

        commitTable = injector.getInstance(CommitTable.class);

    }

    private void setupHBase() throws Exception {
        LOG.info("********** Setting up HBase **********");
        hbaseTestUtil = new HBaseTestingUtility(hbaseConf);
        LOG.info("********** Creating HBase MiniCluster **********");
        hbaseCluster = hbaseTestUtil.startMiniCluster(1);
    }

    private void createRequiredHBaseTables() throws IOException {
        createTableIfNotExists(TIMESTAMP_TABLE_DEFAULT_NAME, TSO_FAMILY);

        createTableIfNotExists(COMMIT_TABLE_DEFAULT_NAME, COMMIT_TABLE_FAMILY, LOW_WATERMARK_FAMILY);
    }

    private void createTableIfNotExists(String tableName, byte[]... families) throws IOException {
        if (!admin.tableExists(tableName)) {
            LOG.info("Creating {} table...", tableName);
            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));

            for (byte[] family : families) {
                HColumnDescriptor datafam = new HColumnDescriptor(family);
                datafam.setMaxVersions(MAX_VERSIONS);
                desc.addFamily(datafam);
            }

            desc.addCoprocessor("org.apache.hadoop.hbase.coprocessor.AggregateImplementation");
            admin.createTable(desc);
            for (byte[] family : families) {
                CompactorUtil.enableOmidCompaction(hbaseConf, TableName.valueOf(tableName), family);
            }
        }

    }

    private void setupTSO() throws IOException, InterruptedException {
        tso = injector.getInstance(TSOServer.class);
        tso.startAndWait();
        TestUtils.waitForSocketListening("localhost", 1234, 100);
        Thread.currentThread().setName("UnitTest(s) thread");
    }

    @AfterClass
    public void cleanupTestCompation() throws Exception {
        teardownTSO();
        hbaseCluster.shutdown();
    }

    private void teardownTSO() throws IOException, InterruptedException {
        tso.stopAndWait();
        TestUtils.waitForSocketNotListening("localhost", 1234, 1000);
    }

    @BeforeMethod
    public void setupTestCompactionIndividualTest() throws Exception {
        randomGenerator = new Random(0xfeedcafeL);
        tm = spy((AbstractTransactionManager) newTransactionManager());
    }

    private TransactionManager newTransactionManager() throws Exception {
        HBaseOmidClientConfiguration hbaseOmidClientConf = HBaseOmidClientConfiguration.create();
        hbaseOmidClientConf.setConnectionString("localhost:1234");
        hbaseOmidClientConf.setHBaseConfiguration(hbaseConf);
        return HBaseTransactionManager.builder(hbaseOmidClientConf)
                .commitTableClient(commitTable.getClient().get())
                .build();
    }

    @Test
    public void testStandardTXsWithShadowCellsAndWithSTBelowAndAboveLWMArePresevedAfterCompaction() throws Throwable {
        String TEST_TABLE = "testStandardTXsWithShadowCellsAndWithSTBelowAndAboveLWMArePresevedAfterCompaction";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable txTable = new TTable(hbaseConf, TEST_TABLE);

        final int ROWS_TO_ADD = 5;

        long fakeAssignedLowWatermark = 0L;
        for (int i = 0; i < ROWS_TO_ADD; ++i) {
            long rowId = randomGenerator.nextLong();
            Transaction tx = tm.begin();
            if (i == (ROWS_TO_ADD / 2)) {
                fakeAssignedLowWatermark = tx.getTransactionId();
                LOG.info("AssignedLowWatermark " + fakeAssignedLowWatermark);
            }
            Put put = new Put(Bytes.toBytes(rowId));
            put.add(fam, qual, data);
            txTable.put(tx, put);
            tm.commit(tx);
        }

        LOG.info("Flushing table {}", TEST_TABLE);
        admin.flush(TEST_TABLE);

        // Return a LWM that triggers compaction & stays between 1 and the max start timestamp assigned to previous TXs
        LOG.info("Regions in table {}: {}", TEST_TABLE, hbaseCluster.getRegions(Bytes.toBytes(TEST_TABLE)).size());
        OmidCompactor omidCompactor = (OmidCompactor) hbaseCluster.getRegions(Bytes.toBytes(TEST_TABLE)).get(0)
                .getCoprocessorHost().findCoprocessor(OmidCompactor.class.getName());
        CommitTable commitTable = injector.getInstance(CommitTable.class);
        CommitTable.Client commitTableClient = spy(commitTable.getClient().get());
        SettableFuture<Long> f = SettableFuture.create();
        f.set(fakeAssignedLowWatermark);
        doReturn(f).when(commitTableClient).readLowWatermark();
        omidCompactor.commitTableClientQueue.add(commitTableClient);
        LOG.info("Compacting table {}", TEST_TABLE);
        admin.majorCompact(TEST_TABLE);

        LOG.info("Sleeping for 3 secs");
        Thread.sleep(3000);
        LOG.info("Waking up after 3 secs");

        // No rows should have been discarded after compacting
        assertEquals("Rows in table after compacting should be " + ROWS_TO_ADD, ROWS_TO_ADD, rowCount(TEST_TABLE, fam));
    }

    @Test
    public void testTXWithoutShadowCellsAndWithSTBelowLWMGetsShadowCellHealedAfterCompaction() throws Exception {
        String TEST_TABLE = "testTXWithoutShadowCellsAndWithSTBelowLWMGetsShadowCellHealedAfterCompaction";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable txTable = new TTable(hbaseConf, TEST_TABLE);

        // The following line emulates a crash after commit that is observed in (*) below
        doThrow(new RuntimeException()).when(tm).updateShadowCells(any(HBaseTransaction.class));

        HBaseTransaction problematicTx = (HBaseTransaction) tm.begin();

        long row = randomGenerator.nextLong();

        // Test shadow cell are created properly
        Put put = new Put(Bytes.toBytes(row));
        put.add(fam, qual, data);
        txTable.put(problematicTx, put);
        try {
            tm.commit(problematicTx);
        } catch (Exception e) { // (*) Crash
            // Do nothing
        }

        assertTrue("Cell should be there",
                CellUtils.hasCell(Bytes.toBytes(row),
                        fam,
                        qual,
                        problematicTx.getStartTimestamp(),
                        new TTableCellGetterAdapter(txTable)));
        assertFalse("Shadow cell should not be there",
                CellUtils.hasShadowCell(Bytes.toBytes(row),
                        fam,
                        qual,
                        problematicTx.getStartTimestamp(),
                        new TTableCellGetterAdapter(txTable)));

        // Return a LWM that triggers compaction and has all the possible start timestamps below it
        LOG.info("Regions in table {}: {}", TEST_TABLE, hbaseCluster.getRegions(Bytes.toBytes(TEST_TABLE)).size());
        OmidCompactor omidCompactor = (OmidCompactor) hbaseCluster.getRegions(Bytes.toBytes(TEST_TABLE)).get(0)
                .getCoprocessorHost().findCoprocessor(OmidCompactor.class.getName());
        CommitTable commitTable = injector.getInstance(CommitTable.class);
        CommitTable.Client commitTableClient = spy(commitTable.getClient().get());
        SettableFuture<Long> f = SettableFuture.create();
        f.set(Long.MAX_VALUE);
        doReturn(f).when(commitTableClient).readLowWatermark();
        omidCompactor.commitTableClientQueue.add(commitTableClient);

        LOG.info("Flushing table {}", TEST_TABLE);
        admin.flush(TEST_TABLE);

        LOG.info("Compacting table {}", TEST_TABLE);
        admin.majorCompact(TEST_TABLE);

        LOG.info("Sleeping for 3 secs");
        Thread.sleep(3000);
        LOG.info("Waking up after 3 secs");

        assertTrue("Cell should be there",
                CellUtils.hasCell(Bytes.toBytes(row),
                        fam,
                        qual,
                        problematicTx.getStartTimestamp(),
                        new TTableCellGetterAdapter(txTable)));
        assertTrue("Shadow cell should not be there",
                CellUtils.hasShadowCell(Bytes.toBytes(row),
                        fam,
                        qual,
                        problematicTx.getStartTimestamp(),
                        new TTableCellGetterAdapter(txTable)));
    }

    @Test
    public void testNeverendingTXsWithSTBelowAndAboveLWMAreDiscardedAndPreservedRespectivelyAfterCompaction()
            throws Throwable {
        String
                TEST_TABLE =
                "testNeverendingTXsWithSTBelowAndAboveLWMAreDiscardedAndPreservedRespectivelyAfterCompaction";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable txTable = new TTable(hbaseConf, TEST_TABLE);

        // The KV in this transaction should be discarded
        HBaseTransaction neverendingTxBelowLowWatermark = (HBaseTransaction) tm.begin();
        long rowId = randomGenerator.nextLong();
        Put put = new Put(Bytes.toBytes(rowId));
        put.add(fam, qual, data);
        txTable.put(neverendingTxBelowLowWatermark, put);
        assertTrue("Cell should be there",
                CellUtils.hasCell(Bytes.toBytes(rowId),
                        fam,
                        qual,
                        neverendingTxBelowLowWatermark.getStartTimestamp(),
                        new TTableCellGetterAdapter(txTable)));
        assertFalse("Shadow cell should not be there",
                CellUtils.hasShadowCell(Bytes.toBytes(rowId),
                        fam,
                        qual,
                        neverendingTxBelowLowWatermark.getStartTimestamp(),
                        new TTableCellGetterAdapter(txTable)));

        // The KV in this transaction should be added without the shadow cells
        HBaseTransaction neverendingTxAboveLowWatermark = (HBaseTransaction) tm.begin();
        long anotherRowId = randomGenerator.nextLong();
        put = new Put(Bytes.toBytes(anotherRowId));
        put.add(fam, qual, data);
        txTable.put(neverendingTxAboveLowWatermark, put);
        assertTrue("Cell should be there",
                CellUtils.hasCell(Bytes.toBytes(anotherRowId),
                        fam,
                        qual,
                        neverendingTxAboveLowWatermark.getStartTimestamp(),
                        new TTableCellGetterAdapter(txTable)));
        assertFalse("Shadow cell should not be there",
                CellUtils.hasShadowCell(Bytes.toBytes(anotherRowId),
                        fam,
                        qual,
                        neverendingTxAboveLowWatermark.getStartTimestamp(),
                        new TTableCellGetterAdapter(txTable)));

        assertEquals("Rows in table before flushing should be 2", 2, rowCount(TEST_TABLE, fam));
        LOG.info("Flushing table {}", TEST_TABLE);
        admin.flush(TEST_TABLE);
        assertEquals("Rows in table after flushing should be 2", 2, rowCount(TEST_TABLE, fam));

        // Return a LWM that triggers compaction and stays between both ST of TXs, so assign 1st TX's start timestamp
        LOG.info("Regions in table {}: {}", TEST_TABLE, hbaseCluster.getRegions(Bytes.toBytes(TEST_TABLE)).size());
        OmidCompactor omidCompactor = (OmidCompactor) hbaseCluster.getRegions(Bytes.toBytes(TEST_TABLE)).get(0)
                .getCoprocessorHost().findCoprocessor(OmidCompactor.class.getName());
        CommitTable commitTable = injector.getInstance(CommitTable.class);
        CommitTable.Client commitTableClient = spy(commitTable.getClient().get());
        SettableFuture<Long> f = SettableFuture.create();
        f.set(neverendingTxBelowLowWatermark.getStartTimestamp());
        doReturn(f).when(commitTableClient).readLowWatermark();
        omidCompactor.commitTableClientQueue.add(commitTableClient);
        LOG.info("Compacting table {}", TEST_TABLE);
        admin.majorCompact(TEST_TABLE);

        LOG.info("Sleeping for 3 secs");
        Thread.sleep(3000);
        LOG.info("Waking up after 3 secs");

        // One row should have been discarded after compacting
        assertEquals("There should be only one row in table after compacting", 1, rowCount(TEST_TABLE, fam));
        // The row from the TX below the LWM should not be there (nor its Shadow Cell)
        assertFalse("Cell should not be there",
                CellUtils.hasCell(Bytes.toBytes(rowId),
                        fam,
                        qual,
                        neverendingTxBelowLowWatermark.getStartTimestamp(),
                        new TTableCellGetterAdapter(txTable)));
        assertFalse("Shadow cell should not be there",
                CellUtils.hasShadowCell(Bytes.toBytes(rowId),
                        fam,
                        qual,
                        neverendingTxBelowLowWatermark.getStartTimestamp(),
                        new TTableCellGetterAdapter(txTable)));
        // The row from the TX above the LWM should be there without the Shadow Cell
        assertTrue("Cell should be there",
                CellUtils.hasCell(Bytes.toBytes(anotherRowId),
                        fam,
                        qual,
                        neverendingTxAboveLowWatermark.getStartTimestamp(),
                        new TTableCellGetterAdapter(txTable)));
        assertFalse("Shadow cell should not be there",
                CellUtils.hasShadowCell(Bytes.toBytes(anotherRowId),
                        fam,
                        qual,
                        neverendingTxAboveLowWatermark.getStartTimestamp(),
                        new TTableCellGetterAdapter(txTable)));

    }

    @Test
    public void testRowsUnalteredWhenCommitTableCannotBeReached() throws Throwable {
        String TEST_TABLE = "testRowsUnalteredWhenCommitTableCannotBeReached";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable txTable = new TTable(hbaseConf, TEST_TABLE);

        // The KV in this transaction should be discarded but in the end should remain there because
        // the commit table won't be accessed (simulating an error on access)
        HBaseTransaction neverendingTx = (HBaseTransaction) tm.begin();
        long rowId = randomGenerator.nextLong();
        Put put = new Put(Bytes.toBytes(rowId));
        put.add(fam, qual, data);
        txTable.put(neverendingTx, put);
        assertTrue("Cell should be there",
                CellUtils.hasCell(Bytes.toBytes(rowId),
                        fam,
                        qual,
                        neverendingTx.getStartTimestamp(),
                        new TTableCellGetterAdapter(txTable)));
        assertFalse("Shadow cell should not be there",
                CellUtils.hasShadowCell(Bytes.toBytes(rowId),
                        fam,
                        qual,
                        neverendingTx.getStartTimestamp(),
                        new TTableCellGetterAdapter(txTable)));

        assertEquals("There should be only one rows in table before flushing", 1, rowCount(TEST_TABLE, fam));
        LOG.info("Flushing table {}", TEST_TABLE);
        admin.flush(TEST_TABLE);
        assertEquals("There should be only one rows in table after flushing", 1, rowCount(TEST_TABLE, fam));

        // Break access to CommitTable functionality in Compactor
        LOG.info("Regions in table {}: {}", TEST_TABLE, hbaseCluster.getRegions(Bytes.toBytes(TEST_TABLE)).size());
        OmidCompactor omidCompactor = (OmidCompactor) hbaseCluster.getRegions(Bytes.toBytes(TEST_TABLE)).get(0)
                .getCoprocessorHost().findCoprocessor(OmidCompactor.class.getName());
        CommitTable commitTable = injector.getInstance(CommitTable.class);
        CommitTable.Client commitTableClient = spy(commitTable.getClient().get());
        SettableFuture<Long> f = SettableFuture.create();
        f.setException(new IOException("Unable to read"));
        doReturn(f).when(commitTableClient).readLowWatermark();
        omidCompactor.commitTableClientQueue.add(commitTableClient);

        LOG.info("Compacting table {}", TEST_TABLE);
        admin.majorCompact(TEST_TABLE); // Should trigger the error when accessing CommitTable funct.

        LOG.info("Sleeping for 3 secs");
        Thread.sleep(3000);
        LOG.info("Waking up after 3 secs");

        // All rows should be there after the failed compaction
        assertEquals("There should be only one row in table after compacting", 1, rowCount(TEST_TABLE, fam));
        assertTrue("Cell should be there",
                CellUtils.hasCell(Bytes.toBytes(rowId),
                        fam,
                        qual,
                        neverendingTx.getStartTimestamp(),
                        new TTableCellGetterAdapter(txTable)));
        assertFalse("Shadow cell should not be there",
                CellUtils.hasShadowCell(Bytes.toBytes(rowId),
                        fam,
                        qual,
                        neverendingTx.getStartTimestamp(),
                        new TTableCellGetterAdapter(txTable)));
    }

    @Test
    public void testOriginalTableParametersAreAvoidedAlsoWhenCompacting() throws Throwable {
        String TEST_TABLE = "testOriginalTableParametersAreAvoidedAlsoWhenCompacting";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable txTable = new TTable(hbaseConf, TEST_TABLE);

        long rowId = randomGenerator.nextLong();
        for (int versionCount = 0; versionCount <= (2 * MAX_VERSIONS); versionCount++) {
            Transaction tx = tm.begin();
            Put put = new Put(Bytes.toBytes(rowId));
            put.add(fam, qual, Bytes.toBytes("testWrite-" + versionCount));
            txTable.put(tx, put);
            tm.commit(tx);
        }

        Transaction tx = tm.begin();
        Get get = new Get(Bytes.toBytes(rowId));
        get.setMaxVersions(2 * MAX_VERSIONS);
        assertEquals("Max versions should be set to " + (2 * MAX_VERSIONS), (2 * MAX_VERSIONS), get.getMaxVersions());
        get.addColumn(fam, qual);
        Result result = txTable.get(tx, get);
        tm.commit(tx);
        List<Cell> column = result.getColumnCells(fam, qual);
        assertEquals("There should be only one version in the result", 1, column.size());

        assertEquals("There should be only one row in table before flushing", 1, rowCount(TEST_TABLE, fam));
        LOG.info("Flushing table {}", TEST_TABLE);
        admin.flush(TEST_TABLE);
        assertEquals("There should be only one row in table after flushing", 1, rowCount(TEST_TABLE, fam));

        // Return a LWM that triggers compaction
        compactEverything(TEST_TABLE);

        // One row should have been discarded after compacting
        assertEquals("There should be only one row in table after compacting", 1, rowCount(TEST_TABLE, fam));

        tx = tm.begin();
        get = new Get(Bytes.toBytes(rowId));
        get.setMaxVersions(2 * MAX_VERSIONS);
        assertEquals("Max versions should be set to " + (2 * MAX_VERSIONS), (2 * MAX_VERSIONS), get.getMaxVersions());
        get.addColumn(fam, qual);
        result = txTable.get(tx, get);
        tm.commit(tx);
        column = result.getColumnCells(fam, qual);
        assertEquals("There should be only one version in the result", 1, column.size());
        assertEquals("Values don't match",
                "testWrite-" + (2 * MAX_VERSIONS),
                Bytes.toString(CellUtil.cloneValue(column.get(0))));
    }

    // manually flush the regions on the region server.
    // flushing like this prevents compaction running
    // directly after the flush, which we want to avoid.
    private void manualFlush(String tableName) throws Throwable {
        LOG.info("Manually flushing all regions");
        HBaseShims.flushAllOnlineRegions(hbaseTestUtil.getHBaseCluster().getRegionServer(0),
                TableName.valueOf(tableName));
    }

    @Test
    public void testOldCellsAreDiscardedAfterCompaction() throws Exception {
        String TEST_TABLE = "testOldCellsAreDiscardedAfterCompaction";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable txTable = new TTable(hbaseConf, TEST_TABLE);

        byte[] rowId = Bytes.toBytes("row");

        // Create 3 transactions modifying the same cell in a particular row
        HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
        Put put1 = new Put(rowId);
        put1.add(fam, qual, Bytes.toBytes("testValue 1"));
        txTable.put(tx1, put1);
        tm.commit(tx1);

        HBaseTransaction tx2 = (HBaseTransaction) tm.begin();
        Put put2 = new Put(rowId);
        put2.add(fam, qual, Bytes.toBytes("testValue 2"));
        txTable.put(tx2, put2);
        tm.commit(tx2);

        HBaseTransaction tx3 = (HBaseTransaction) tm.begin();
        Put put3 = new Put(rowId);
        put3.add(fam, qual, Bytes.toBytes("testValue 3"));
        txTable.put(tx3, put3);
        tm.commit(tx3);

        // Before compaction, the three timestamped values for the cell should be there
        TTableCellGetterAdapter getter = new TTableCellGetterAdapter(txTable);
        assertTrue("Put cell of Tx1 should be there",
                CellUtils.hasCell(rowId, fam, qual, tx1.getStartTimestamp(), getter));
        assertTrue("Put shadow cell of Tx1 should be there",
                CellUtils.hasShadowCell(rowId, fam, qual, tx1.getStartTimestamp(), getter));
        assertTrue("Put cell of Tx2 cell should be there",
                CellUtils.hasCell(rowId, fam, qual, tx2.getStartTimestamp(), getter));
        assertTrue("Put shadow cell of Tx2 should be there",
                CellUtils.hasShadowCell(rowId, fam, qual, tx2.getStartTimestamp(), getter));
        assertTrue("Put cell of Tx3 cell should be there",
                CellUtils.hasCell(rowId, fam, qual, tx3.getStartTimestamp(), getter));
        assertTrue("Put shadow cell of Tx3 should be there",
                CellUtils.hasShadowCell(rowId, fam, qual, tx3.getStartTimestamp(), getter));

        // Compact
        HBaseTransaction lwmTx = (HBaseTransaction) tm.begin();
        compactWithLWM(lwmTx.getStartTimestamp(), TEST_TABLE);

        // After compaction, only the last value for the cell should have survived
        assertFalse("Put cell of Tx1 should not be there",
                CellUtils.hasCell(rowId, fam, qual, tx1.getStartTimestamp(), getter));
        assertFalse("Put shadow cell of Tx1 should not be there",
                CellUtils.hasShadowCell(rowId, fam, qual, tx1.getStartTimestamp(), getter));
        assertFalse("Put cell of Tx2 should not be there",
                CellUtils.hasCell(rowId, fam, qual, tx2.getStartTimestamp(), getter));
        assertFalse("Put shadow cell of Tx2 should not be there",
                CellUtils.hasShadowCell(rowId, fam, qual, tx2.getStartTimestamp(), getter));
        assertTrue("Put cell of Tx3 cell should be there",
                CellUtils.hasCell(rowId, fam, qual, tx3.getStartTimestamp(), getter));
        assertTrue("Put shadow cell of Tx3 should be there",
                CellUtils.hasShadowCell(rowId, fam, qual, tx3.getStartTimestamp(), getter));

        // A new transaction after compaction should read the last value written
        HBaseTransaction newTx1 = (HBaseTransaction) tm.begin();
        Get newGet1 = new Get(rowId);
        newGet1.addColumn(fam, qual);
        Result result = txTable.get(newTx1, newGet1);
        assertEquals(Bytes.toBytes("testValue 3"), result.getValue(fam, qual));
        // Write a new value
        Put newPut1 = new Put(rowId);
        newPut1.add(fam, qual, Bytes.toBytes("new testValue 1"));
        txTable.put(newTx1, newPut1);

        // Start a second new transaction
        HBaseTransaction newTx2 = (HBaseTransaction) tm.begin();
        // Commit first of the new tx
        tm.commit(newTx1);

        // The second transaction should still read the previous value
        Get newGet2 = new Get(rowId);
        newGet2.addColumn(fam, qual);
        result = txTable.get(newTx2, newGet2);
        assertEquals(Bytes.toBytes("testValue 3"), result.getValue(fam, qual));
        tm.commit(newTx2);

        // Only two values -the new written by newTx1 and the last value
        // for the cell after compaction- should have survived
        assertFalse("Put cell of Tx1 should not be there",
                CellUtils.hasCell(rowId, fam, qual, tx1.getStartTimestamp(), getter));
        assertFalse("Put shadow cell of Tx1 should not be there",
                CellUtils.hasShadowCell(rowId, fam, qual, tx1.getStartTimestamp(), getter));
        assertFalse("Put cell of Tx2 should not be there",
                CellUtils.hasCell(rowId, fam, qual, tx2.getStartTimestamp(), getter));
        assertFalse("Put shadow cell of Tx2 should not be there",
                CellUtils.hasShadowCell(rowId, fam, qual, tx2.getStartTimestamp(), getter));
        assertTrue("Put cell of Tx3 cell should be there",
                CellUtils.hasCell(rowId, fam, qual, tx3.getStartTimestamp(), getter));
        assertTrue("Put shadow cell of Tx3 should be there",
                CellUtils.hasShadowCell(rowId, fam, qual, tx3.getStartTimestamp(), getter));
        assertTrue("Put cell of NewTx1 cell should be there",
                CellUtils.hasCell(rowId, fam, qual, newTx1.getStartTimestamp(), getter));
        assertTrue("Put shadow cell of NewTx1 should be there",
                CellUtils.hasShadowCell(rowId, fam, qual, newTx1.getStartTimestamp(), getter));
    }

    /**
     * Tests a case where a temporary failure to flush causes the compactor to crash
     */
    @Test
    public void testDuplicateDeletes() throws Throwable {
        String TEST_TABLE = "testDuplicateDeletes";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable txTable = new TTable(hbaseConf, TEST_TABLE);

        // jump through hoops to trigger a minor compaction.
        // a minor compaction will only run if there are enough
        // files to be compacted, but that is less than the number
        // of total files, in which case it will run a major
        // compaction. The issue this is testing only shows up
        // with minor compaction, as only Deletes can be duplicate
        // and major compactions filter them out.
        byte[] firstRow = "FirstRow".getBytes();
        HBaseTransaction tx0 = (HBaseTransaction) tm.begin();
        Put put0 = new Put(firstRow);
        put0.add(fam, qual, Bytes.toBytes("testWrite-1"));
        txTable.put(tx0, put0);
        tm.commit(tx0);

        // create the first hfile
        manualFlush(TEST_TABLE);

        // write a row, it won't be committed
        byte[] rowToBeCompactedAway = "compactMe".getBytes();
        HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
        Put put1 = new Put(rowToBeCompactedAway);
        put1.add(fam, qual, Bytes.toBytes("testWrite-1"));
        txTable.put(tx1, put1);
        txTable.flushCommits();

        // write a row to trigger the double delete problem
        byte[] row = "iCauseErrors".getBytes();
        HBaseTransaction tx2 = (HBaseTransaction) tm.begin();
        Put put2 = new Put(row);
        put2.add(fam, qual, Bytes.toBytes("testWrite-1"));
        txTable.put(tx2, put2);
        tm.commit(tx2);

        HBaseTransaction tx3 = (HBaseTransaction) tm.begin();
        Put put3 = new Put(row);
        put3.add(fam, qual, Bytes.toBytes("testWrite-1"));
        txTable.put(tx3, put3);
        txTable.flushCommits();

        // cause a failure on HBaseTM#preCommit();
        Set<HBaseCellId> writeSet = tx3.getWriteSet();
        assertEquals(1, writeSet.size());
        List<HBaseCellId> newWriteSet = new ArrayList<>();
        final AtomicBoolean flushFailing = new AtomicBoolean(true);
        for (HBaseCellId id : writeSet) {
            HTableInterface failableHTable = spy(id.getTable());
            doAnswer(new Answer<Void>() {
                @Override
                public Void answer(InvocationOnMock invocation)
                        throws Throwable {
                    if (flushFailing.get()) {
                        throw new RetriesExhaustedWithDetailsException(new ArrayList<Throwable>(),
                                new ArrayList<Row>(), new ArrayList<String>());
                    } else {
                        invocation.callRealMethod();
                    }
                    return null;
                }
            }).when(failableHTable).flushCommits();

            newWriteSet.add(new HBaseCellId(failableHTable,
                    id.getRow(), id.getFamily(),
                    id.getQualifier(), id.getTimestamp()));
        }
        writeSet.clear();
        writeSet.addAll(newWriteSet);

        try {
            tm.commit(tx3);
            fail("Shouldn't succeed");
        } catch (TransactionException tme) {
            flushFailing.set(false);
            tm.rollback(tx3);
        }

        // create second hfile,
        // it should contain multiple deletes
        manualFlush(TEST_TABLE);

        // create loads of files
        byte[] anotherRow = "someotherrow".getBytes();
        HBaseTransaction tx4 = (HBaseTransaction) tm.begin();
        Put put4 = new Put(anotherRow);
        put4.add(fam, qual, Bytes.toBytes("testWrite-1"));
        txTable.put(tx4, put4);
        tm.commit(tx4);

        // create third hfile
        manualFlush(TEST_TABLE);

        // trigger minor compaction and give it time to run
        setCompactorLWM(tx4.getStartTimestamp(), TEST_TABLE);
        admin.compact(TEST_TABLE);
        Thread.sleep(3000);

        // check if the cell that should be compacted, is compacted
        assertFalse("Cell should not be be there",
                CellUtils.hasCell(rowToBeCompactedAway, fam, qual,
                        tx1.getStartTimestamp(),
                        new TTableCellGetterAdapter(txTable)));
    }

    @Test(timeOut = 60000)
    public void testNonOmidCFIsUntouched() throws Throwable {
        String TEST_TABLE = "testNonOmidCFIsUntouched";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable txTable = new TTable(hbaseConf, TEST_TABLE);

        admin.disableTable(TEST_TABLE);
        byte[] nonOmidCF = Bytes.toBytes("nonOmidCF");
        byte[] nonOmidQual = Bytes.toBytes("nonOmidCol");
        HColumnDescriptor nonomidfam = new HColumnDescriptor(nonOmidCF);
        nonomidfam.setMaxVersions(MAX_VERSIONS);
        admin.addColumn(TEST_TABLE, nonomidfam);
        admin.enableTable(TEST_TABLE);

        byte[] rowId = Bytes.toBytes("testRow");
        Transaction tx = tm.begin();
        Put put = new Put(rowId);
        put.add(fam, qual, Bytes.toBytes("testValue"));
        txTable.put(tx, put);

        Put nonTxPut = new Put(rowId);
        nonTxPut.add(nonOmidCF, nonOmidQual, Bytes.toBytes("nonTxVal"));
        txTable.getHTable().put(nonTxPut);
        txTable.flushCommits(); // to make sure it left the client

        Get g = new Get(rowId);
        Result result = txTable.getHTable().get(g);
        assertEquals("Should be there, precompact",
                1, result.getColumnCells(nonOmidCF, nonOmidQual).size());
        assertEquals("Should be there, precompact",
                1, result.getColumnCells(fam, qual).size());

        compactEverything(TEST_TABLE);

        result = txTable.getHTable().get(g);
        assertEquals("Should be there, postcompact",
                1, result.getColumnCells(nonOmidCF, nonOmidQual).size());
        assertEquals("Should not be there, postcompact",
                0, result.getColumnCells(fam, qual).size());
    }

    // ************************************************************************
    // Tests on tombstones and non-transactional Deletes
    // ************************************************************************

    /**
     * Test that when a major compaction runs, cells that were deleted non-transactionally dissapear
     */
    @Test(timeOut = 60000)
    public void testACellDeletedNonTransactionallyDoesNotAppearWhenAMajorCompactionOccurs() throws Throwable {
        String TEST_TABLE = "testACellDeletedNonTransactionallyDoesNotAppearWhenAMajorCompactionOccurs";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable txTable = new TTable(hbaseConf, TEST_TABLE);

        HTable table = new HTable(hbaseConf, TEST_TABLE);

        // Write first a value transactionally
        HBaseTransaction tx0 = (HBaseTransaction) tm.begin();
        byte[] rowId = Bytes.toBytes("row1");
        Put p0 = new Put(rowId);
        p0.add(fam, qual, Bytes.toBytes("testValue-0"));
        txTable.put(tx0, p0);
        tm.commit(tx0);

        // Then perform a non-transactional Delete
        Delete d = new Delete(rowId);
        d.deleteColumn(fam, qual);
        table.delete(d);

        // Trigger a major compaction
        HBaseTransaction lwmTx = (HBaseTransaction) tm.begin();
        compactWithLWM(lwmTx.getStartTimestamp(), TEST_TABLE);

        // Then perform a non-tx (raw) scan...
        Scan scan = new Scan();
        scan.setRaw(true);
        ResultScanner scannerResults = table.getScanner(scan);

        // ...and test the deleted cell is not there anymore
        assertNull("There should be no results in scan results", scannerResults.next());

        table.close();

    }

    /**
     * Test that when a minor compaction runs, cells that were deleted non-transactionally are preserved. This is to
     * allow users still access the cells when doing "improper" operations on a transactional table
     */
    @Test(timeOut = 60000)
    public void testACellDeletedNonTransactionallyIsPreservedWhenMinorCompactionOccurs() throws Throwable {
        String TEST_TABLE = "testACellDeletedNonTransactionallyIsPreservedWhenMinorCompactionOccurs";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable txTable = new TTable(hbaseConf, TEST_TABLE);

        HTable table = new HTable(hbaseConf, TEST_TABLE);

        // Configure the environment to create a minor compaction

        // Write first a value transactionally
        HBaseTransaction tx0 = (HBaseTransaction) tm.begin();
        byte[] rowId = Bytes.toBytes("row1");
        Put p0 = new Put(rowId);
        p0.add(fam, qual, Bytes.toBytes("testValue-0"));
        txTable.put(tx0, p0);
        tm.commit(tx0);

        // create the first hfile
        manualFlush(TEST_TABLE);

        // Write another value transactionally
        HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
        Put p1 = new Put(rowId);
        p1.add(fam, qual, Bytes.toBytes("testValue-1"));
        txTable.put(tx1, p1);
        tm.commit(tx1);

        // create the second hfile
        manualFlush(TEST_TABLE);

        // Then perform a non-transactional Delete
        Delete d = new Delete(rowId);
        d.deleteColumn(fam, qual);
        table.delete(d);

        // create the third hfile
        manualFlush(TEST_TABLE);

        // Trigger the minor compaction
        HBaseTransaction lwmTx = (HBaseTransaction) tm.begin();
        setCompactorLWM(lwmTx.getStartTimestamp(), TEST_TABLE);
        admin.compact(TEST_TABLE);
        Thread.sleep(5000);

        // Then perform a non-tx (raw) scan...
        Scan scan = new Scan();
        scan.setRaw(true);
        ResultScanner scannerResults = table.getScanner(scan);

        // ...and test the deleted cell is still there
        int count = 0;
        Result scanResult;
        List<Cell> listOfCellsScanned = new ArrayList<>();
        while ((scanResult = scannerResults.next()) != null) {
            listOfCellsScanned = scanResult.listCells(); // equivalent to rawCells()
            count++;
        }
        assertEquals("There should be only one result in scan results", 1, count);
        assertEquals("There should be three cell entries in the scan results (2 puts, 1 delete)", 3,
                listOfCellsScanned.size());
        boolean wasDeletedCellFound = false;
        int numberOfDeletedCellsFound = 0;
        for (Cell cell : listOfCellsScanned) {
            if (CellUtil.isDelete(cell)) {
                wasDeletedCellFound = true;
                numberOfDeletedCellsFound++;
            }
        }
        assertTrue("We should have found a non-transactionally deleted cell", wasDeletedCellFound);
        assertEquals("There should be only only one deleted cell", 1, numberOfDeletedCellsFound);

        table.close();
    }

    /**
     * Test that when a minor compaction runs, tombstones are not cleaned up
     */
    @Test(timeOut = 60000)
    public void testTombstonesAreNotCleanedUpWhenMinorCompactionOccurs() throws Throwable {
        String TEST_TABLE = "testTombstonesAreNotCleanedUpWhenMinorCompactionOccurs";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable txTable = new TTable(hbaseConf, TEST_TABLE);

        // Configure the environment to create a minor compaction

        HBaseTransaction tx0 = (HBaseTransaction) tm.begin();
        byte[] rowId = Bytes.toBytes("case1");
        Put p = new Put(rowId);
        p.add(fam, qual, Bytes.toBytes("testValue-0"));
        txTable.put(tx0, p);
        tm.commit(tx0);

        // create the first hfile
        manualFlush(TEST_TABLE);

        // Create the tombstone
        HBaseTransaction deleteTx = (HBaseTransaction) tm.begin();
        Delete d = new Delete(rowId);
        d.deleteColumn(fam, qual);
        txTable.delete(deleteTx, d);
        tm.commit(deleteTx);

        // create the second hfile
        manualFlush(TEST_TABLE);

        HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
        Put p1 = new Put(rowId);
        p1.add(fam, qual, Bytes.toBytes("testValue-11"));
        txTable.put(tx1, p1);
        tm.commit(tx1);

        // create the third hfile
        manualFlush(TEST_TABLE);

        HBaseTransaction lastTx = (HBaseTransaction) tm.begin();
        Put p2 = new Put(rowId);
        p2.add(fam, qual, Bytes.toBytes("testValue-222"));
        txTable.put(lastTx, p2);
        tm.commit(lastTx);

        // Trigger the minor compaction
        HBaseTransaction lwmTx = (HBaseTransaction) tm.begin();
        setCompactorLWM(lwmTx.getStartTimestamp(), TEST_TABLE);
        admin.compact(TEST_TABLE);
        Thread.sleep(5000);

        // Checks on results after compaction
        TTableCellGetterAdapter getter = new TTableCellGetterAdapter(txTable);
        assertFalse("Put cell should be there",
                CellUtils.hasCell(rowId, fam, qual,
                        tx0.getStartTimestamp(), getter));
        assertFalse("Put shadow cell should be there",
                CellUtils.hasShadowCell(rowId, fam, qual,
                        tx0.getStartTimestamp(), getter));
        assertTrue("Put cell should be there",
                CellUtils.hasCell(rowId, fam, qual,
                        tx1.getStartTimestamp(), getter));
        assertTrue("Put shadow cell should be there",
                CellUtils.hasShadowCell(rowId, fam, qual,
                        tx1.getStartTimestamp(), getter));
        assertTrue("Delete cell should be there",
                CellUtils.hasCell(rowId, fam, qual,
                        deleteTx.getStartTimestamp(), getter));
        assertTrue("Delete shadow cell should be there",
                CellUtils.hasShadowCell(rowId, fam, qual,
                        deleteTx.getStartTimestamp(), getter));
        assertTrue("Put cell should be there",
                CellUtils.hasCell(rowId, fam, qual,
                        lastTx.getStartTimestamp(), getter));
        assertTrue("Put shadow cell should be there",
                CellUtils.hasShadowCell(rowId, fam, qual,
                        lastTx.getStartTimestamp(), getter));
    }


    /**
     * Test that when compaction runs, tombstones are cleaned up case1: 1 put (ts < lwm) then tombstone (ts > lwm)
     */
    @Test(timeOut = 60000)
    public void testTombstonesAreCleanedUpCase1() throws Exception {
        String TEST_TABLE = "testTombstonesAreCleanedUpCase1";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable txTable = new TTable(hbaseConf, TEST_TABLE);

        HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
        byte[] rowId = Bytes.toBytes("case1");
        Put p = new Put(rowId);
        p.add(fam, qual, Bytes.toBytes("testValue"));
        txTable.put(tx1, p);
        tm.commit(tx1);

        HBaseTransaction lwmTx = (HBaseTransaction) tm.begin();
        setCompactorLWM(lwmTx.getStartTimestamp(), TEST_TABLE);

        HBaseTransaction tx2 = (HBaseTransaction) tm.begin();
        Delete d = new Delete(rowId);
        d.deleteColumn(fam, qual);
        txTable.delete(tx2, d);
        tm.commit(tx2);

        TTableCellGetterAdapter getter = new TTableCellGetterAdapter(txTable);
        assertTrue("Put cell should be there",
                CellUtils.hasCell(rowId, fam, qual,
                        tx1.getStartTimestamp(), getter));
        assertTrue("Put shadow cell should be there",
                CellUtils.hasShadowCell(rowId, fam, qual,
                        tx1.getStartTimestamp(), getter));
        assertTrue("Delete cell should be there",
                CellUtils.hasCell(rowId, fam, qual,
                        tx2.getStartTimestamp(), getter));
        assertTrue("Delete shadow cell should be there",
                CellUtils.hasShadowCell(rowId, fam, qual,
                        tx2.getStartTimestamp(), getter));
    }

    /**
     * Test that when compaction runs, tombstones are cleaned up case2: 1 put (ts < lwm) then tombstone (ts < lwm)
     */
    @Test(timeOut = 60000)
    public void testTombstonesAreCleanedUpCase2() throws Exception {
        String TEST_TABLE = "testTombstonesAreCleanedUpCase2";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable txTable = new TTable(hbaseConf, TEST_TABLE);

        HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
        byte[] rowId = Bytes.toBytes("case2");
        Put p = new Put(rowId);
        p.add(fam, qual, Bytes.toBytes("testValue"));
        txTable.put(tx1, p);
        tm.commit(tx1);

        HBaseTransaction tx2 = (HBaseTransaction) tm.begin();
        Delete d = new Delete(rowId);
        d.deleteColumn(fam, qual);
        txTable.delete(tx2, d);
        tm.commit(tx2);

        HBaseTransaction lwmTx = (HBaseTransaction) tm.begin();
        compactWithLWM(lwmTx.getStartTimestamp(), TEST_TABLE);

        TTableCellGetterAdapter getter = new TTableCellGetterAdapter(txTable);
        assertFalse("Put cell shouldn't be there",
                CellUtils.hasCell(rowId, fam, qual,
                        tx1.getStartTimestamp(), getter));
        assertFalse("Put shadow cell shouldn't be there",
                CellUtils.hasShadowCell(rowId, fam, qual,
                        tx1.getStartTimestamp(), getter));
        assertFalse("Delete cell shouldn't be there",
                CellUtils.hasCell(rowId, fam, qual,
                        tx2.getStartTimestamp(), getter));
        assertFalse("Delete shadow cell shouldn't be there",
                CellUtils.hasShadowCell(rowId, fam, qual,
                        tx2.getStartTimestamp(), getter));
    }

    /**
     * Test that when compaction runs, tombstones are cleaned up case3: 1 put (ts < lwm) then tombstone (ts < lwm) not
     * committed
     */
    @Test(timeOut = 60000)
    public void testTombstonesAreCleanedUpCase3() throws Exception {
        String TEST_TABLE = "testTombstonesAreCleanedUpCase3";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable txTable = new TTable(hbaseConf, TEST_TABLE);

        HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
        byte[] rowId = Bytes.toBytes("case3");
        Put p = new Put(rowId);
        p.add(fam, qual, Bytes.toBytes("testValue"));
        txTable.put(tx1, p);
        tm.commit(tx1);

        HBaseTransaction tx2 = (HBaseTransaction) tm.begin();
        Delete d = new Delete(rowId);
        d.deleteColumn(fam, qual);
        txTable.delete(tx2, d);

        HBaseTransaction lwmTx = (HBaseTransaction) tm.begin();
        compactWithLWM(lwmTx.getStartTimestamp(), TEST_TABLE);

        TTableCellGetterAdapter getter = new TTableCellGetterAdapter(txTable);
        assertTrue("Put cell should be there",
                CellUtils.hasCell(rowId, fam, qual,
                        tx1.getStartTimestamp(), getter));
        assertTrue("Put shadow cell shouldn't be there",
                CellUtils.hasShadowCell(rowId, fam, qual,
                        tx1.getStartTimestamp(), getter));
        assertFalse("Delete cell shouldn't be there",
                CellUtils.hasCell(rowId, fam, qual,
                        tx2.getStartTimestamp(), getter));
        assertFalse("Delete shadow cell shouldn't be there",
                CellUtils.hasShadowCell(rowId, fam, qual,
                        tx2.getStartTimestamp(), getter));
    }

    /**
     * Test that when compaction runs, tombstones are cleaned up case4: 1 put (ts < lwm) then tombstone (ts > lwm) not
     * committed
     */
    @Test(timeOut = 60000)
    public void testTombstonesAreCleanedUpCase4() throws Exception {
        String TEST_TABLE = "testTombstonesAreCleanedUpCase4";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable txTable = new TTable(hbaseConf, TEST_TABLE);

        HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
        byte[] rowId = Bytes.toBytes("case4");
        Put p = new Put(rowId);
        p.add(fam, qual, Bytes.toBytes("testValue"));
        txTable.put(tx1, p);
        tm.commit(tx1);

        HBaseTransaction lwmTx = (HBaseTransaction) tm.begin();

        HBaseTransaction tx2 = (HBaseTransaction) tm.begin();
        Delete d = new Delete(rowId);
        d.deleteColumn(fam, qual);
        txTable.delete(tx2, d);
        compactWithLWM(lwmTx.getStartTimestamp(), TEST_TABLE);

        TTableCellGetterAdapter getter = new TTableCellGetterAdapter(txTable);
        assertTrue("Put cell should be there",
                CellUtils.hasCell(rowId, fam, qual,
                        tx1.getStartTimestamp(), getter));
        assertTrue("Put shadow cell shouldn't be there",
                CellUtils.hasShadowCell(rowId, fam, qual,
                        tx1.getStartTimestamp(), getter));
        assertTrue("Delete cell should be there",
                CellUtils.hasCell(rowId, fam, qual,
                        tx2.getStartTimestamp(), getter));
        assertFalse("Delete shadow cell shouldn't be there",
                CellUtils.hasShadowCell(rowId, fam, qual,
                        tx2.getStartTimestamp(), getter));
    }

    /**
     * Test that when compaction runs, tombstones are cleaned up case5: tombstone (ts < lwm)
     */
    @Test(timeOut = 60000)
    public void testTombstonesAreCleanedUpCase5() throws Exception {
        String TEST_TABLE = "testTombstonesAreCleanedUpCase5";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable txTable = new TTable(hbaseConf, TEST_TABLE);

        HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
        byte[] rowId = Bytes.toBytes("case5");
        Delete d = new Delete(rowId);
        d.deleteColumn(fam, qual);
        txTable.delete(tx1, d);
        tm.commit(tx1);

        HBaseTransaction lwmTx = (HBaseTransaction) tm.begin();
        compactWithLWM(lwmTx.getStartTimestamp(), TEST_TABLE);

        TTableCellGetterAdapter getter = new TTableCellGetterAdapter(txTable);
        assertFalse("Delete cell shouldn't be there",
                CellUtils.hasCell(rowId, fam, qual,
                        tx1.getStartTimestamp(), getter));
        assertFalse("Delete shadow cell shouldn't be there",
                CellUtils.hasShadowCell(rowId, fam, qual,
                        tx1.getStartTimestamp(), getter));
    }

    /**
     * Test that when compaction runs, tombstones are cleaned up case6: tombstone (ts < lwm), then put (ts < lwm)
     */
    @Test(timeOut = 60000)
    public void testTombstonesAreCleanedUpCase6() throws Exception {
        String TEST_TABLE = "testTombstonesAreCleanedUpCase6";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable txTable = new TTable(hbaseConf, TEST_TABLE);
        byte[] rowId = Bytes.toBytes("case6");

        HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
        Delete d = new Delete(rowId);
        d.deleteColumn(fam, qual);
        txTable.delete(tx1, d);
        tm.commit(tx1);

        HBaseTransaction tx2 = (HBaseTransaction) tm.begin();
        Put p = new Put(rowId);
        p.add(fam, qual, Bytes.toBytes("testValue"));
        txTable.put(tx2, p);
        tm.commit(tx2);

        HBaseTransaction lwmTx = (HBaseTransaction) tm.begin();
        compactWithLWM(lwmTx.getStartTimestamp(), TEST_TABLE);

        TTableCellGetterAdapter getter = new TTableCellGetterAdapter(txTable);
        assertFalse("Delete cell shouldn't be there",
                CellUtils.hasCell(rowId, fam, qual,
                        tx1.getStartTimestamp(), getter));
        assertFalse("Delete shadow cell shouldn't be there",
                CellUtils.hasShadowCell(rowId, fam, qual,
                        tx1.getStartTimestamp(), getter));
        assertTrue("Put cell should be there",
                CellUtils.hasCell(rowId, fam, qual,
                        tx2.getStartTimestamp(), getter));
        assertTrue("Put shadow cell shouldn't be there",
                CellUtils.hasShadowCell(rowId, fam, qual,
                        tx2.getStartTimestamp(), getter));
    }

    private void setCompactorLWM(long lwm, String tableName) throws Exception {
        OmidCompactor omidCompactor = (OmidCompactor) hbaseCluster.getRegions(Bytes.toBytes(tableName)).get(0)
                .getCoprocessorHost().findCoprocessor(OmidCompactor.class.getName());
        CommitTable commitTable = injector.getInstance(CommitTable.class);
        CommitTable.Client commitTableClient = spy(commitTable.getClient().get());
        SettableFuture<Long> f = SettableFuture.create();
        f.set(lwm);
        doReturn(f).when(commitTableClient).readLowWatermark();
        omidCompactor.commitTableClientQueue.add(commitTableClient);
    }

    private void compactEverything(String tableName) throws Exception {
        compactWithLWM(Long.MAX_VALUE, tableName);
    }

    private void compactWithLWM(long lwm, String tableName) throws Exception {
        admin.flush(tableName);

        LOG.info("Regions in table {}: {}", tableName, hbaseCluster.getRegions(Bytes.toBytes(tableName)).size());
        setCompactorLWM(lwm, tableName);
        LOG.info("Compacting table {}", tableName);
        admin.majorCompact(tableName);

        LOG.info("Sleeping for 3 secs");
        Thread.sleep(3000);
        LOG.info("Waking up after 3 secs");
    }

    private long rowCount(String tableName, byte[] family) throws Throwable {
        Scan scan = new Scan();
        scan.addFamily(family);
        return aggregationClient.rowCount(TableName.valueOf(tableName), new LongColumnInterpreter(), scan);
    }

}
