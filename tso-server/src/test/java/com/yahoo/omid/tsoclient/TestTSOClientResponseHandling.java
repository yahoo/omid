package com.yahoo.omid.tsoclient;

import com.yahoo.omid.tso.ProgrammableTSOServer;
import com.yahoo.omid.tso.ProgrammableTSOServer.AbortResponse;
import com.yahoo.omid.tso.ProgrammableTSOServer.CommitResponse;
import com.yahoo.omid.tso.ProgrammableTSOServer.TimestampResponse;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static org.testng.Assert.assertEquals;

public class TestTSOClientResponseHandling {

    private static final int TSO_PORT = 4321;
    private static final long START_TS = 1L;
    private static final long COMMIT_TS = 2L;

    private ProgrammableTSOServer tsoServer = new ProgrammableTSOServer(TSO_PORT);
    // Client under test
    private TSOClient tsoClient;

    @BeforeClass
    public void configureAndCreateClient() {

        OmidClientConfiguration tsoClientConf = OmidClientConfiguration.create();
        tsoClientConf.setConnectionString("localhost:" + TSO_PORT);
        tsoClient = TSOClient.newInstance(tsoClientConf);
    }

    @BeforeMethod
    public void reset() {
        tsoServer.cleanResponses();
    }

    @Test
    public void testTimestampRequestReceivingASuccessfulResponse() throws Exception {
        // test request timestamp response returns a timestamp

        // Program the TSO to return an ad-hoc Timestamp response
        tsoServer.queueResponse(new TimestampResponse(START_TS));

        long startTS = tsoClient.getNewStartTimestamp().get();
        assertEquals(startTS, START_TS);
    }

    @Test
    public void testCommitRequestReceivingAnAbortResponse() throws Exception {
        // test commit request which is aborted on the server side
        // (e.g. due to conflicts with other transaction) throws an
        // execution exception with an AbortException as a cause

        // Program the TSO to return an Abort response
        tsoServer.queueResponse(new AbortResponse(START_TS));

        try {
            tsoClient.commit(START_TS, Collections.<CellId>emptySet()).get();
        } catch (ExecutionException ee) {
            assertEquals(ee.getCause().getClass(), AbortException.class);
        }
    }

    @Test
    public void testCommitRequestReceivingASuccessfulResponse() throws Exception {
        // test commit request which is successfully committed on the server
        // side returns a commit timestamp

        // Program the TSO to return an Commit response (with no required heuristic actions)
        tsoServer.queueResponse(new CommitResponse(false, START_TS, COMMIT_TS));

        long commitTS = tsoClient.commit(START_TS, Collections.<CellId>emptySet()).get();
        assertEquals(commitTS, COMMIT_TS);
    }

    @Test
    public void testCommitRequestReceivingAHeuristicResponse() throws Exception {
        // test commit request which needs heuristic actions from the client
        // throws an execution exception with a NewTSOException as a cause

        // Program the TSO to return an Commit response requiring heuristic actions
        tsoServer.queueResponse(new CommitResponse(true, START_TS, COMMIT_TS));
        try {
            tsoClient.commit(START_TS, Collections.<CellId>emptySet()).get();
        } catch (ExecutionException ee) {
            assertEquals(ee.getCause().getClass(), NewTSOException.class);
        }

    }

}
