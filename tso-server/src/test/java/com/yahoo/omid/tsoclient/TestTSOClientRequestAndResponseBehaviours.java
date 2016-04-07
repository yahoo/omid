package com.yahoo.omid.tsoclient;

import com.google.common.collect.Sets;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.yahoo.omid.TestUtils;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.proto.TSOProto;
import com.yahoo.omid.tso.PausableTimestampOracle;
import com.yahoo.omid.tso.TSOMockModule;
import com.yahoo.omid.tso.TSOServer;
import com.yahoo.omid.tso.TSOServerConfig;
import com.yahoo.omid.tso.TimestampOracle;
import com.yahoo.omid.tso.util.DummyCellIdImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestTSOClientRequestAndResponseBehaviours {

    private static final Logger LOG = LoggerFactory.getLogger(TestTSOClientRequestAndResponseBehaviours.class);

    private static final String TSO_SERVER_HOST = "localhost";
    private static final int TSO_SERVER_PORT = 1234;

    private final static CellId c1 = new DummyCellIdImpl(0xdeadbeefL);
    private final static CellId c2 = new DummyCellIdImpl(0xfeedcafeL);

    private final static Set<CellId> testWriteSet = Sets.newHashSet(c1, c2);

    private OmidClientConfiguration tsoClientConf;

    // Required infrastructure for TSOClient test
    private TSOServer tsoServer;
    private PausableTimestampOracle pausableTSOracle;
    private CommitTable commitTable;

    @BeforeClass
    public void setup() throws Exception {

        TSOServerConfig tsoConfig = new TSOServerConfig();
        tsoConfig.setMaxItems(1000);
        tsoConfig.setPort(TSO_SERVER_PORT);
        Module tsoServerMockModule = new TSOMockModule(tsoConfig);
        Injector injector = Guice.createInjector(tsoServerMockModule);

        LOG.info("==================================================================================================");
        LOG.info("======================================= Init TSO Server ==========================================");
        LOG.info("==================================================================================================");

        tsoServer = injector.getInstance(TSOServer.class);
        tsoServer.startAndWait();
        TestUtils.waitForSocketListening(TSO_SERVER_HOST, TSO_SERVER_PORT, 100);

        LOG.info("==================================================================================================");
        LOG.info("===================================== TSO Server Initialized =====================================");
        LOG.info("==================================================================================================");

        pausableTSOracle = (PausableTimestampOracle) injector.getInstance(TimestampOracle.class);
        commitTable = injector.getInstance(CommitTable.class);

    }

    @AfterClass
    public void tearDown() throws Exception {

        tsoServer.stopAndWait();
        tsoServer = null;
        TestUtils.waitForSocketNotListening(TSO_SERVER_HOST, TSO_SERVER_PORT, 1000);

    }

    @BeforeMethod
    public void beforeMethod() {
        OmidClientConfiguration tsoClientConf = new OmidClientConfiguration();
        tsoClientConf.setConnectionString(TSO_SERVER_HOST + ":" + TSO_SERVER_PORT);

        this.tsoClientConf = tsoClientConf;

    }

    @AfterMethod
    public void afterMethod() {

        pausableTSOracle.resume();

    }

    /**
     * Test to ensure TSOClient timeouts are cancelled.
     * At some point a bug was detected because the TSOClient timeouts were not cancelled, and as timestamp requests
     * had no way to be correlated to timestamp responses, random requests were just timed out after a certain time.
     * We send a lot of timestamp requests, and wait for them to complete.
     * Ensure that the next request doesn't get hit by the timeouts of the previous
     * requests. (i.e. make sure we cancel timeouts)
     */
    @Test(timeOut = 30_000)
    public void testTimeoutsAreCancelled() throws Exception {

        TSOClient client = TSOClient.newInstance(tsoClientConf);
        int requestTimeoutInMs = 500;
        int requestMaxRetries = 5;
        LOG.info("Request timeout {} ms; Max retries {}", requestTimeoutInMs, requestMaxRetries);
        Future<Long> f = null;
        for (int i = 0; i < (requestMaxRetries * 10); i++) {
            f = client.getNewStartTimestamp();
        }
        if (f != null) {
            f.get();
        }
        pausableTSOracle.pause();
        long msToSleep = ((long) (requestTimeoutInMs * 0.75));
        LOG.info("Sleeping for {} ms", msToSleep);
        TimeUnit.MILLISECONDS.sleep(msToSleep);
        f = client.getNewStartTimestamp();
        msToSleep = ((long) (requestTimeoutInMs * 0.9));
        LOG.info("Sleeping for {} ms", msToSleep);
        TimeUnit.MILLISECONDS.sleep(msToSleep);
        LOG.info("Resuming");
        pausableTSOracle.resume();
        f.get();

    }

    @Test(timeOut = 30_000)
    public void testCommitGetsServiceUnavailableExceptionWhenCommunicationFails() throws Exception {

        OmidClientConfiguration testTSOClientConf = new OmidClientConfiguration();
        testTSOClientConf.setConnectionString(TSO_SERVER_HOST + ":" + TSO_SERVER_PORT);
        testTSOClientConf.setRequestMaxRetries(0);
        TSOClient client = TSOClient.newInstance(testTSOClientConf);

        List<Long> startTimestamps = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            startTimestamps.add(client.getNewStartTimestamp().get());
        }

        pausableTSOracle.pause();

        List<Future<Long>> futures = new ArrayList<>();
        for (long s : startTimestamps) {
            futures.add(client.commit(s, Sets.<CellId>newHashSet()));
        }
        TSOClientAccessor.closeChannel(client);

        for (Future<Long> f : futures) {
            try {
                f.get();
                fail("Shouldn't be able to complete");
            } catch (ExecutionException ee) {
                assertTrue(ee.getCause() instanceof ServiceUnavailableException,
                           "Should be a service unavailable exception");
            }
        }
    }

    /**
     * Test that if a client tries to make a request without handshaking, it will be disconnected.
     */
    @Test(timeOut = 30_000)
    public void testHandshakeBetweenOldClientAndCurrentServer() throws Exception {

        TSOClientRaw raw = new TSOClientRaw(TSO_SERVER_HOST, TSO_SERVER_PORT);

        TSOProto.Request request = TSOProto.Request.newBuilder()
                .setTimestampRequest(TSOProto.TimestampRequest.newBuilder().build())
                .build();
        raw.write(request);
        try {
            raw.getResponse().get();
            fail("Channel should be closed");
        } catch (ExecutionException ee) {
            assertEquals(ee.getCause().getClass(), ConnectionException.class, "Should be channel closed exception");
        }
        raw.close();

    }

    // ----------------------------------------------------------------------------------------------------------------
    // Test duplicate commits
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * This tests the case where messages arrive at the TSO out of order. This can happen in the case
     * the channel get dropped and the retry is done in a new channel. However, the TSO will respond with
     * aborted to the original message because the retry was already committed and it would be prohibitively
     * expensive to check all non-retry requests to see if they are already committed. For this reason
     * a client must ensure that if it is sending a retry due to a socket error, the previous channel
     * must be entirely closed so that it will not actually receive the abort response. TCP guarantees
     * that this doesn't happen in non-socket error cases.
     *
     */
    @Test(timeOut = 30_000)
    public void testOutOfOrderMessages() throws Exception {

        TSOClient client = TSOClient.newInstance(tsoClientConf);
        TSOClientOneShot clientOneShot = new TSOClientOneShot(TSO_SERVER_HOST, TSO_SERVER_PORT);

        long ts1 = client.getNewStartTimestamp().get();

        TSOProto.Response response1 = clientOneShot.makeRequest(createCommitRequest(ts1, true, testWriteSet));
        TSOProto.Response response2 = clientOneShot.makeRequest(createCommitRequest(ts1, false, testWriteSet));
        assertFalse(response1.getCommitResponse().getAborted(), "Retry Transaction should commit");
        assertTrue(response2.getCommitResponse().getAborted(), "Transaction should abort");
    }

    @Test(timeOut = 30_000)
    public void testDuplicateCommitAborting() throws Exception {

        TSOClient client = TSOClient.newInstance(tsoClientConf);
        TSOClientOneShot clientOneShot = new TSOClientOneShot(TSO_SERVER_HOST, TSO_SERVER_PORT);

        long ts1 = client.getNewStartTimestamp().get();
        long ts2 = client.getNewStartTimestamp().get();
        client.commit(ts2, testWriteSet).get();

        TSOProto.Response response1 = clientOneShot.makeRequest(createCommitRequest(ts1, false, testWriteSet));
        TSOProto.Response response2 = clientOneShot.makeRequest(createCommitRequest(ts1, true, testWriteSet));
        assertTrue(response1.getCommitResponse().getAborted(), "Transaction should abort");
        assertTrue(response2.getCommitResponse().getAborted(), "Retry commit should abort");
    }

    @Test(timeOut = 30_000)
    public void testDuplicateCommit() throws Exception {

        TSOClient client = TSOClient.newInstance(tsoClientConf);
        TSOClientOneShot clientOneShot = new TSOClientOneShot(TSO_SERVER_HOST, TSO_SERVER_PORT);

        long ts1 = client.getNewStartTimestamp().get();

        TSOProto.Response response1 = clientOneShot.makeRequest(createCommitRequest(ts1, false, testWriteSet));
        TSOProto.Response response2 = clientOneShot.makeRequest(createCommitRequest(ts1, true, testWriteSet));
        assertEquals(response2.getCommitResponse().getCommitTimestamp(),
                     response1.getCommitResponse().getCommitTimestamp(),
                     "Commit timestamp should be the same");
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Test TSOClient retry behaviour
    // ----------------------------------------------------------------------------------------------------------------

    @Test(timeOut = 30_000)
    public void testCommitCanSucceedWhenChannelDisconnected() throws Exception {

        TSOClient client = TSOClient.newInstance(tsoClientConf);

        long ts1 = client.getNewStartTimestamp().get();
        pausableTSOracle.pause();
        TSOFuture<Long> future = client.commit(ts1, testWriteSet);
        TSOClientAccessor.closeChannel(client);
        pausableTSOracle.resume();
        future.get();

    }

    @Test(timeOut = 30_000)
    public void testCommitCanSucceedWithMultipleTimeouts() throws Exception {

        OmidClientConfiguration testTSOClientConf = new OmidClientConfiguration();
        testTSOClientConf.setConnectionString(TSO_SERVER_HOST + ":" + TSO_SERVER_PORT);
        testTSOClientConf.setRequestTimeoutInMs(100);
        testTSOClientConf.setRequestMaxRetries(10000);
        TSOClient client = TSOClient.newInstance(testTSOClientConf);

        long ts1 = client.getNewStartTimestamp().get();
        pausableTSOracle.pause();
        TSOFuture<Long> future = client.commit(ts1, testWriteSet);
        TimeUnit.SECONDS.sleep(1);
        pausableTSOracle.resume();
        future.get();
    }

    @Test(timeOut = 30_000)
    public void testCommitFailWhenTSOIsDown() throws Exception {

        OmidClientConfiguration testTSOClientConf = new OmidClientConfiguration();
        testTSOClientConf.setConnectionString(TSO_SERVER_HOST + ":" + TSO_SERVER_PORT);
        testTSOClientConf.setRequestTimeoutInMs(100);
        testTSOClientConf.setRequestMaxRetries(10);
        TSOClient client = TSOClient.newInstance(testTSOClientConf);

        long ts1 = client.getNewStartTimestamp().get();
        pausableTSOracle.pause();
        TSOFuture<Long> future = client.commit(ts1, testWriteSet);
        try {
            future.get();
        } catch (ExecutionException e) {
            assertEquals(e.getCause().getClass(), ServiceUnavailableException.class,
                         "Should be a ServiceUnavailableExeption");
        }

    }

    @Test(timeOut = 30_000)
    public void testTimestampRequestSucceedWithMultipleTimeouts() throws Exception {

        OmidClientConfiguration testTSOClientConf = new OmidClientConfiguration();
        testTSOClientConf.setConnectionString(TSO_SERVER_HOST + ":" + TSO_SERVER_PORT);
        testTSOClientConf.setRequestTimeoutInMs(100);
        testTSOClientConf.setRequestMaxRetries(10000);
        TSOClient client = TSOClient.newInstance(testTSOClientConf);

        pausableTSOracle.pause();
        Future<Long> future = client.getNewStartTimestamp();
        TimeUnit.SECONDS.sleep(1);
        pausableTSOracle.resume();
        future.get();

    }

    // ----------------------------------------------------------------------------------------------------------------
    // The next 3 tests are similar to the ones in TestRetryProcessor but checking the result on the TSOClient side
    // (They exercise the communication protocol) TODO Remove???
    // ----------------------------------------------------------------------------------------------------------------
    @Test
    public void testCommitTimestampPresentInCommitTableReturnsCommit() throws Exception {

        TSOClient client = TSOClient.newInstance(tsoClientConf);
        TSOClientOneShot clientOneShot = new TSOClientOneShot(TSO_SERVER_HOST, TSO_SERVER_PORT);

        long tx1ST = client.getNewStartTimestamp().get();

        clientOneShot.makeRequest(createRetryCommitRequest(tx1ST));
        TSOProto.Response response = clientOneShot.makeRequest(createRetryCommitRequest(tx1ST));
        assertFalse(response.getCommitResponse().getAborted(), "Transaction should be committed");
        assertFalse(response.getCommitResponse().getMakeHeuristicDecision());
        assertEquals(response.getCommitResponse().getCommitTimestamp(), tx1ST + 1);
    }

    @Test
    public void testInvalidCommitTimestampPresentInCommitTableReturnsAbort() throws Exception {

        TSOClient client = TSOClient.newInstance(tsoClientConf);
        TSOClientOneShot clientOneShot = new TSOClientOneShot(TSO_SERVER_HOST, TSO_SERVER_PORT);

        long tx1ST = client.getNewStartTimestamp().get();
        // Invalidate the transaction
        commitTable.getClient().tryInvalidateTransaction(tx1ST);

        clientOneShot.makeRequest(createRetryCommitRequest(tx1ST));
        TSOProto.Response response = clientOneShot.makeRequest(createRetryCommitRequest(tx1ST));
        assertTrue(response.getCommitResponse().getAborted(), "Transaction should be aborted");
        assertFalse(response.getCommitResponse().getMakeHeuristicDecision());
        assertEquals(response.getCommitResponse().getCommitTimestamp(), 0);
    }

    @Test
    public void testCommitTimestampNotPresentInCommitTableReturnsAnAbort() throws Exception {

        TSOClient client = TSOClient.newInstance(tsoClientConf);
        TSOClientOneShot clientOneShot = new TSOClientOneShot(TSO_SERVER_HOST, TSO_SERVER_PORT);

        long tx1ST = client.getNewStartTimestamp().get();

        clientOneShot.makeRequest(createRetryCommitRequest(tx1ST));

        // Simulate remove entry from the commit table before exercise retry
        commitTable.getClient().completeTransaction(tx1ST);

        TSOProto.Response response = clientOneShot.makeRequest(createRetryCommitRequest(tx1ST));
        assertTrue(response.getCommitResponse().getAborted(), "Transaction should abort");
        assertFalse(response.getCommitResponse().getMakeHeuristicDecision());
        assertEquals(response.getCommitResponse().getCommitTimestamp(), 0);
    }
    // ----------------------------------------------------------------------------------------------------------------
    // The previous 3 tests are similar to the ones in TestRetryProcessor but checking the result on the TSOClient side
    // (They exercise the communication protocol) TODO Remove???
    // ----------------------------------------------------------------------------------------------------------------

    // ----------------------------------------------------------------------------------------------------------------
    // Helper methods
    // ----------------------------------------------------------------------------------------------------------------

    private TSOProto.Request createRetryCommitRequest(long ts) {
        return createCommitRequest(ts, true, testWriteSet);
    }

    private TSOProto.Request createCommitRequest(long ts, boolean retry, Set<CellId> writeSet) {
        TSOProto.Request.Builder builder = TSOProto.Request.newBuilder();
        TSOProto.CommitRequest.Builder commitBuilder = TSOProto.CommitRequest.newBuilder();
        commitBuilder.setStartTimestamp(ts);
        commitBuilder.setIsRetry(retry);
        for (CellId cell : writeSet) {
            commitBuilder.addCellId(cell.getCellId());
        }
        return builder.setCommitRequest(commitBuilder.build()).build();
    }

}
