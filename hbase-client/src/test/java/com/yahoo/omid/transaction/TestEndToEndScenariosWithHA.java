package com.yahoo.omid.transaction;

import com.google.common.base.Charsets;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.yahoo.omid.TestUtils;
import com.yahoo.omid.tso.LeaseManagement;
import com.yahoo.omid.tso.PausableLeaseManager;
import com.yahoo.omid.tso.TSOServer;
import com.yahoo.omid.tso.TSOServerCommandLineConfig;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.yahoo.omid.ZKConstants.CURRENT_TSO_PATH;
import static com.yahoo.omid.ZKConstants.OMID_NAMESPACE;
import static com.yahoo.omid.ZKConstants.TSO_LEASE_PATH;
import static com.yahoo.omid.timestamp.storage.HBaseTimestampStorage.TIMESTAMP_TABLE_DEFAULT_NAME;
import static com.yahoo.omid.timestamp.storage.HBaseTimestampStorage.TSO_FAMILY;
import static com.yahoo.omid.tsoclient.TSOClientConfiguration.ConnType.ZK;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_RETRIES_NUMBER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(groups = "sharedHBase")
public class TestEndToEndScenariosWithHA extends OmidTestBase {

    private static final int TEST_LEASE_PERIOD_MS = 1000;

    private static final Logger LOG = LoggerFactory.getLogger(TestEndToEndScenariosWithHA.class);

    private static final byte[] qualifier1 = Bytes.toBytes("test-q1");
    private static final byte[] qualifier2 = Bytes.toBytes("test-q2l");
    private static final byte[] row1 = Bytes.toBytes("row1");
    private static final byte[] row2 = Bytes.toBytes("row2");
    private static final byte[] initialData = Bytes.toBytes("testWrite-0");
    private static final byte[] data1_q1 = Bytes.toBytes("testWrite-1-q1");
    private static final byte[] data1_q2 = Bytes.toBytes("testWrite-1-q2");
    private static final byte[] data2_q1 = Bytes.toBytes("testWrite-2-q1");
    private static final byte[] data2_q2 = Bytes.toBytes("testWrite-2-q2");
    private static final int TSO1_PORT = 2223;
    private static final int TSO2_PORT = 4321;

    private CountDownLatch barrierTillTSOAddressPublication;


    private CuratorFramework zkClient;

    private TSOServer tso1;
    private TSOServer tso2;

    private PausableLeaseManager leaseManager1;


    private TransactionManager tm;

    @BeforeMethod(alwaysRun = true, timeOut = 30_000)
    public void setup() throws Exception {
        // Get the zkConnection string from minicluster
        String zkConnection = "localhost:" + hBaseUtils.getZkCluster().getClientPort();

        zkClient = provideInitializedZookeeperClient(zkConnection);

        // Synchronize TSO start
        barrierTillTSOAddressPublication = new CountDownLatch(1);
        final NodeCache currentTSOZNode = new NodeCache(zkClient, CURRENT_TSO_PATH);
        currentTSOZNode.getListenable().addListener(new NodeCacheListener() {

            @Override
            public void nodeChanged() throws Exception {
                byte[] currentTSOAndEpochAsBytes = currentTSOZNode.getCurrentData().getData();
                String currentTSOAndEpoch = new String(currentTSOAndEpochAsBytes, Charsets.UTF_8);
                if (currentTSOAndEpoch.endsWith("#0")) { // Wait till a TSO instance publishes the epoch
                    barrierTillTSOAddressPublication.countDown();
                }
            }

        });
        currentTSOZNode.start(true);

        // Configure TSO 1
        String[] configArgs = new String[]{"-port", Integer.toString(TSO1_PORT), "-maxItems", "1000"};
        TSOServerCommandLineConfig config1 = TSOServerCommandLineConfig.parseConfig(configArgs);
        config1.shouldHostAndPortBePublishedInZK = true;
        config1.setZKCluster(zkConnection);
        config1.setLeasePeriodInMs(TEST_LEASE_PERIOD_MS);
        Injector injector1 = Guice.createInjector(new TestTSOModule(hbaseConf, config1));
        LOG.info("===================== Starting TSO 1 =====================");
        tso1 = injector1.getInstance(TSOServer.class);
        leaseManager1 = (PausableLeaseManager) injector1.getInstance(LeaseManagement.class);
        tso1.startAndWait();
        TestUtils.waitForSocketListening("localhost", TSO1_PORT, 100);
        LOG.info("================ Finished loading TSO 1 ==================");

        // Configure TSO 2
        configArgs = new String[]{"-port", Integer.toString(TSO2_PORT), "-maxItems", "1000"};
        TSOServerCommandLineConfig config2 = TSOServerCommandLineConfig.parseConfig(configArgs);
        config2.shouldHostAndPortBePublishedInZK = true;
        config2.setZKCluster(zkConnection);
        config2.setLeasePeriodInMs(TEST_LEASE_PERIOD_MS);
        Injector injector2 = Guice.createInjector(new TestTSOModule(hbaseConf, config2));
        LOG.info("===================== Starting TSO 2 =====================");
        tso2 = injector2.getInstance(TSOServer.class);
        PausableLeaseManager leaseManager2 = (PausableLeaseManager) injector2.getInstance(LeaseManagement.class);
        tso2.startAndWait();
        // Don't do this here: TestUtils.waitForSocketListening("localhost", 4321, 100);
        LOG.info("================ Finished loading TSO 2 ==================");

        // Wait till the master TSO is up
        barrierTillTSOAddressPublication.await();
        currentTSOZNode.close();

        // Configure HBase TM
        LOG.info("===================== Starting TM =====================");
        HBaseOmidClientConfiguration hbaseOmidClientConf = HBaseOmidClientConfiguration.create();
        hbaseOmidClientConf.setConnectionType(ZK);
        hbaseOmidClientConf.setConnectionString(zkConnection);
        hbaseConf.setInt(HBASE_CLIENT_RETRIES_NUMBER, 3);
        tm = HBaseTransactionManager.builder(hbaseOmidClientConf).hbaseConf(hbaseConf).build();
        LOG.info("===================== TM Started =========================");
    }


    @AfterMethod(alwaysRun = true, timeOut = 30_000)
    public void cleanup() throws Exception {
        LOG.info("Cleanup");
        HBaseAdmin admin = hBaseUtils.getHBaseAdmin();
        deleteTable(admin, TableName.valueOf(TIMESTAMP_TABLE_DEFAULT_NAME));
        hBaseUtils
                .createTable(Bytes.toBytes(TIMESTAMP_TABLE_DEFAULT_NAME), new byte[][]{TSO_FAMILY}, Integer.MAX_VALUE);
        tso1.stopAndWait();
        TestUtils.waitForSocketNotListening("localhost", TSO1_PORT, 100);
        tso2.stopAndWait();
        TestUtils.waitForSocketNotListening("localhost", TSO2_PORT, 100);

        zkClient.delete().forPath(TSO_LEASE_PATH);
        LOG.info("ZKPath {} deleted", TSO_LEASE_PATH);
        zkClient.delete().forPath(CURRENT_TSO_PATH);
        LOG.info("ZKPaths {} deleted", CURRENT_TSO_PATH);

        zkClient.close();
    }

    //
    // TSO 1 is MASTER & TSO 2 is BACKUP
    // Setup: TX 0 -> Add initial data to cells R1C1 (v0) & R2C2 (v0)
    // TX 1 starts (TSO1)
    // TX 1 modifies cells R1C1 & R2C2 (v1)
    // Interleaved Read TX -IR TX- starts (TSO1)
    // TSO 1 PAUSES -> TSO 2 becomes MASTER
    // IR TX reads R1C1 -> should get v0
    // TX 1 tries to commit -> should abort because was started in TSO 1
    // IR TX reads R2C2 -> should get v0
    // IR TX tries to commit -> should abort because was started in TSO 1
    // End of Test state: R1C1 & R2C2 (v0)
    @Test(timeOut = 60_000)
    public void testScenario1() throws Exception {
        try (TTable txTable = new TTable(hbaseConf, TEST_TABLE)) {

            // Write initial values for the test
            HBaseTransaction tx0 = (HBaseTransaction) tm.begin();
            long initialEpoch = tx0.getEpoch();
            LOG.info("Starting Tx {} writing initial values for cells ({}) ", tx0, Bytes.toString(initialData));
            Put putInitialDataRow1 = new Put(row1);
            putInitialDataRow1.add(TEST_FAMILY.getBytes(), qualifier1, initialData);
            txTable.put(tx0, putInitialDataRow1);
            Put putInitialDataRow2 = new Put(row2);
            putInitialDataRow2.add(TEST_FAMILY.getBytes(), qualifier2, initialData);
            txTable.put(tx0, putInitialDataRow2);
            tm.commit(tx0);

            // Initial checks
            checkRowValues(txTable, initialData, initialData);

            HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
            LOG.info("Starting Tx {} writing values for cells ({}, {}) ", tx1, Bytes.toString(data1_q1),
                    Bytes.toString(data1_q2));
            Put putData1R1Q1 = new Put(row1);
            putData1R1Q1.add(TEST_FAMILY.getBytes(), qualifier1, data1_q1);
            txTable.put(tx1, putData1R1Q1);
            Put putData1R2Q2 = new Put(row2);
            putData1R2Q2.add(TEST_FAMILY.getBytes(), qualifier2, data1_q2);
            txTable.put(tx1, putData1R2Q2);

            Transaction interleavedReadTx = tm.begin();

            LOG.info("Starting Interleaving Read Tx {} for checking cell values", interleavedReadTx.getTransactionId());

            // Simulate a GC pause to change mastership (should throw a ServiceUnavailable exception)
            LOG.info("++++++++++++++++++++++++++++++++++++++++++++++++++++++");
            LOG.info("++++++++++++++++++++++++++++++++++++++++++++++++++++++");
            LOG.info("++++++++++++++++++++ PAUSING TSO 1 +++++++++++++++++++");
            LOG.info("++++++++++++++++++++++++++++++++++++++++++++++++++++++");
            LOG.info("++++++++++++++++++++++++++++++++++++++++++++++++++++++");
            leaseManager1.pausedInStillInLeasePeriod();

            // Read interleaved and check the values writen by tx 1
            Get getRow1 = new Get(row1).setMaxVersions(1);
            getRow1.addColumn(TEST_FAMILY.getBytes(), qualifier1);
            Result r = txTable.get(interleavedReadTx, getRow1);
            assertEquals(r.getValue(TEST_FAMILY.getBytes(), qualifier1), initialData,
                    "Unexpected value for SI read R1Q1" + interleavedReadTx + ": "
                            + Bytes.toString(r.getValue(TEST_FAMILY.getBytes(), qualifier1)));

            // Try to commit, but it should abort due to the change in mastership
            try {
                tm.commit(tx1);
                fail();
            } catch (RollbackException e) {
                // Expected
                LOG.info("Rollback cause for Tx {}: ", tx1, e.getCause());
                assertEquals(tx1.getStatus(), Transaction.Status.ROLLEDBACK);
                assertEquals(tx1.getEpoch(), initialEpoch);
            }

            // Read interleaved and check the values writen by tx 1
            Get getRow2 = new Get(row2).setMaxVersions(1);
            r = txTable.get(interleavedReadTx, getRow2);
            assertEquals(r.getValue(TEST_FAMILY.getBytes(), qualifier2), initialData,
                    "Unexpected value for SI read R2Q2" + interleavedReadTx + ": "
                            + Bytes.toString(r.getValue(TEST_FAMILY.getBytes(), qualifier2)));

            try {
                tm.commit(interleavedReadTx);
                fail();
            } catch (RollbackException e) {
                // Expected
                // NOTE: This is a read only transaction (It's writeset is empty)
                // and probably it should not be aborted despite the change in
                // TSO mastership. We should take this into consideration for future improvements
                LOG.info("Rollback cause for Tx {}: ", interleavedReadTx, e.getCause());
                assertEquals(interleavedReadTx.getEpoch(), initialEpoch);
                assertEquals(interleavedReadTx.getStatus(), Transaction.Status.ROLLEDBACK);
            }

            LOG.info("Sleep the Lease period till the client is informed about"
                    + "the new TSO connection parameters and how can connect");
            TestUtils.waitForSocketListening("localhost", TSO2_PORT, 100);

            checkRowValues(txTable, initialData, initialData);

            // Need to resume to let other test progress
            leaseManager1.resume();

        }

    }

    //
    // TSO 1 is MASTER & TSO 2 is BACKUP
    // Setup: TX 0 -> Add initial data to cells R1C1 (v0) & R2C2 (v0)
    // TX 1 starts (TSO1)
    // TX 1 modifies cells R1C1 & R2C2 (v1)
    // TSO 1 is KILLED -> TSO 2 becomes MASTER
    // TX 1 tries to commit -> should abort because was started in TSO 1
    // TX 2 starts (TSO1)
    // TX 2 reads R1C1 -> should get v0
    // TX 2 reads R2C2 -> should get v0
    // TX 2 modifies cells R1C1 & R2C2 (v2)
    // TX 2 commits
    // End of Test state: R1C1 & R2C2 (v2)
    @Test(timeOut = 60_000)
    public void testScenario2() throws Exception {
        try (TTable txTable = new TTable(hbaseConf, TEST_TABLE)) {

            // Write initial values for the test
            HBaseTransaction tx0 = (HBaseTransaction) tm.begin();
            long initialEpoch = tx0.getEpoch();
            LOG.info("Starting Tx {} writing initial values for cells ({}) ", tx0, Bytes.toString(initialData));
            Put putInitialDataRow1 = new Put(row1);
            putInitialDataRow1.add(TEST_FAMILY.getBytes(), qualifier1, initialData);
            txTable.put(tx0, putInitialDataRow1);
            Put putInitialDataRow2 = new Put(row2);
            putInitialDataRow2.add(TEST_FAMILY.getBytes(), qualifier2, initialData);
            txTable.put(tx0, putInitialDataRow2);
            tm.commit(tx0);

            HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
            LOG.info("Starting Tx {} writing values for cells ({}, {}) ", tx1, Bytes.toString(data1_q1),
                    Bytes.toString(data1_q2));
            Put putData1R1Q1 = new Put(row1);
            putData1R1Q1.add(TEST_FAMILY.getBytes(), qualifier1, data1_q1);
            txTable.put(tx1, putData1R1Q1);
            Put putData1R2Q2 = new Put(row2);
            putData1R2Q2.add(TEST_FAMILY.getBytes(), qualifier2, data1_q2);
            txTable.put(tx1, putData1R2Q2);

            // Provoke change in mastership (should throw a Connection exception)
            LOG.info("++++++++++++++++++++++++++++++++++++++++++++++++++++++");
            LOG.info("++++++++++++++++++++++++++++++++++++++++++++++++++++++");
            LOG.info("++++++++++++++++++++ KILLING TSO 1 +++++++++++++++++++");
            LOG.info("++++++++++++++++++++++++++++++++++++++++++++++++++++++");
            LOG.info("++++++++++++++++++++++++++++++++++++++++++++++++++++++");
            tso1.stopAndWait();
            TestUtils.waitForSocketNotListening("localhost", TSO1_PORT, 100);

            // Try to commit, but it should abort due to the change in mastership
            try {
                tm.commit(tx1);
                String failMsg = String.format("%s should not commit. Initial epoch was: %d", tx1, initialEpoch);
                fail(failMsg);
            } catch (RollbackException e) {
                // Expected
                LOG.info("Rollback cause for Tx {}: ", tx1, e.getCause());
                assertEquals(tx1.getStatus(), Transaction.Status.ROLLEDBACK);
                assertEquals(tx1.getEpoch(), initialEpoch);
            }

            LOG.info("Sleep some time till the client is informed about"
                    + "the new TSO connection parameters and how can connect");
            TimeUnit.SECONDS.sleep(10 + 2);

            HBaseTransaction tx2 = (HBaseTransaction) tm.begin();
            LOG.info("Starting Tx {} writing values for cells ({}, {}) ", tx2, Bytes.toString(data1_q1),
                    Bytes.toString(data1_q2));
            Get getData1R1Q1 = new Get(row1).setMaxVersions(1);
            Result r = txTable.get(tx2, getData1R1Q1);
            assertEquals(r.getValue(TEST_FAMILY.getBytes(), qualifier1), initialData,
                    "Unexpected value for SI read R1Q1" + tx2 + ": "
                            + Bytes.toString(r.getValue(TEST_FAMILY.getBytes(), qualifier1)));
            Get getData1R2Q2 = new Get(row2).setMaxVersions(1);
            r = txTable.get(tx2, getData1R2Q2);
            assertEquals(r.getValue(TEST_FAMILY.getBytes(), qualifier2), initialData,
                    "Unexpected value for SI read R1Q1" + tx2 + ": "
                            + Bytes.toString(r.getValue(TEST_FAMILY.getBytes(), qualifier2)));

            Put putData2R1Q1 = new Put(row1);
            putData2R1Q1.add(TEST_FAMILY.getBytes(), qualifier1, data2_q1);
            txTable.put(tx2, putData2R1Q1);
            Put putData2R2Q2 = new Put(row2);
            putData2R2Q2.add(TEST_FAMILY.getBytes(), qualifier2, data2_q2);
            txTable.put(tx2, putData2R2Q2);
            // This one should commit in the new TSO
            tm.commit(tx2);

            assertEquals(tx2.getStatus(), Transaction.Status.COMMITTED);
            assertTrue(tx2.getEpoch() > tx0.getCommitTimestamp());

            checkRowValues(txTable, data2_q1, data2_q2);
        }

    }

    private void checkRowValues(TTable txTable, byte[] expectedDataR1Q1, byte[] expectedDataR2Q2)
            throws TransactionException, IOException, RollbackException {
        Transaction readTx = tm.begin();
        LOG.info("Starting Read Tx {} for checking cell values", readTx.getTransactionId());
        Get getRow1 = new Get(row1).setMaxVersions(1);
        getRow1.addColumn(TEST_FAMILY.getBytes(), qualifier1);
        Result r = txTable.get(readTx, getRow1);
        assertEquals(r.getValue(TEST_FAMILY.getBytes(), qualifier1), expectedDataR1Q1,
                "Unexpected value for SI read R1Q1" + readTx + ": " + Bytes
                        .toString(r.getValue(TEST_FAMILY.getBytes(), qualifier1)));
        Get getRow2 = new Get(row2).setMaxVersions(1);
        r = txTable.get(readTx, getRow2);
        assertEquals(r.getValue(TEST_FAMILY.getBytes(), qualifier2), expectedDataR2Q2,
                "Unexpected value for SI read R2Q2" + readTx + ": " + Bytes
                        .toString(r.getValue(TEST_FAMILY.getBytes(), qualifier2)));
        tm.commit(readTx);
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------------------------------------------------------

    private static CuratorFramework provideInitializedZookeeperClient(String zkConnection) throws Exception {

        LOG.info("Creating Zookeeper Client connecting to {}", zkConnection);

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework zkClient = CuratorFrameworkFactory
                .builder()
                .namespace(OMID_NAMESPACE)
                .connectString(zkConnection)
                .retryPolicy(retryPolicy).build();

        LOG.info("Connecting to ZK cluster {}", zkClient.getState());
        zkClient.start();
        zkClient.blockUntilConnected();
        LOG.info("Connection to ZK cluster {}", zkClient.getState());

        return zkClient;
    }

}
