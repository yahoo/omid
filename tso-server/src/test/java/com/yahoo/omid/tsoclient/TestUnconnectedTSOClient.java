package com.yahoo.omid.tsoclient;

import com.yahoo.omid.tso.util.DummyCellIdImpl;
import com.yahoo.omid.tsoclient.TSOClient.DisconnectedState;
import com.yahoo.statemachine.StateMachine.FsmImpl;
import org.slf4j.Logger;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Sets.newHashSet;
import static org.slf4j.LoggerFactory.getLogger;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

/**
 * Test the behavior of requests on a TSOClient component that is not connected to a TSO server.
 */
public class TestUnconnectedTSOClient {

    private static final Logger LOG = getLogger(TestUnconnectedTSOClient.class);

    private static final int TSO_RECONNECTION_DELAY_IN_SECS_FOR_TEST = 2;

    @Test(timeOut = 30_000) // 30 secs
    public void testRequestsDoneOnAnUnconnectedTSOClientAlwaysReturn() throws Exception {

        OmidClientConfiguration tsoClientConf = OmidClientConfiguration.create();
        tsoClientConf.setConnectionString("localhost:12345");
        tsoClientConf.setReconnectionDelaySecs(TSO_RECONNECTION_DELAY_IN_SECS_FOR_TEST);

        // Component under test
        TSOClient tsoClient = TSOClient.newInstance(tsoClientConf);

        // Internal accessor to fsm
        FsmImpl fsm = (FsmImpl) tsoClient.fsm;

        assertEquals(fsm.getState().getClass(), DisconnectedState.class);

        // Test requests to the 3 relevant methods in TSO client

        try {
            tsoClient.getNewStartTimestamp().get();
            fail();
        } catch (ExecutionException e) {
            LOG.info("Exception expected");
            assertEquals(e.getCause().getClass(), ConnectionException.class);
            TimeUnit.SECONDS.sleep(TSO_RECONNECTION_DELAY_IN_SECS_FOR_TEST * 2);
            assertEquals(fsm.getState().getClass(), DisconnectedState.class);
        }

        try {
            tsoClient.commit(1, newHashSet(new DummyCellIdImpl(0xdeadbeefL))).get();
            fail();
        } catch (ExecutionException e) {
            LOG.info("Exception expected");
            assertEquals(e.getCause().getClass(), ConnectionException.class);
            TimeUnit.SECONDS.sleep(TSO_RECONNECTION_DELAY_IN_SECS_FOR_TEST * 2);
            assertEquals(fsm.getState().getClass(), DisconnectedState.class);
        }

        tsoClient.close().get();
        LOG.info("No exception expected");
    }

}
