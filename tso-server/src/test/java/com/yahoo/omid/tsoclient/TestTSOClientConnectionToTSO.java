package com.yahoo.omid.tsoclient;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.yahoo.omid.TestUtils;
import com.yahoo.omid.tso.TSOMockModule;
import com.yahoo.omid.tso.TSOServer;
import com.yahoo.omid.tso.TSOServerCommandLineConfig;
import com.yahoo.statemachine.StateMachine.FsmImpl;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutionException;

import static com.yahoo.omid.ZKConstants.CURRENT_TSO_PATH;
import static com.yahoo.omid.tsoclient.OmidClientConfiguration.ConnType.ZK;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestTSOClientConnectionToTSO {

    private static final Logger LOG = LoggerFactory.getLogger(TestTSOClientConnectionToTSO.class);

    // Constants and variables for component connectivity
    private static final String TSO_HOST = "localhost";
    private int tsoPortForTest;
    private int zkPortForTest;
    private String zkClusterForTest;

    private Injector injector = null;

    private TestingServer zkServer;

    private CuratorFramework zkClient;
    private TSOServer tsoServer;

    @BeforeMethod
    public void beforeMethod() throws Exception {

        tsoPortForTest = TestUtils.getFreeLocalPort();

        zkPortForTest = TestUtils.getFreeLocalPort();
        zkClusterForTest = TSO_HOST + ":" + zkPortForTest;
        LOG.info("Starting ZK Server in port {}", zkPortForTest);
        zkServer = TestUtils.provideTestingZKServer(zkPortForTest);
        LOG.info("ZK Server Started @ {}", zkServer.getConnectString());

        zkClient = TestUtils.provideConnectedZKClient(zkClusterForTest);

        Stat stat;
        try {
            zkClient.delete().forPath(CURRENT_TSO_PATH);
            stat = zkClient.checkExists().forPath(CURRENT_TSO_PATH);
            assertNull(stat, CURRENT_TSO_PATH + " should not exist");
        } catch (NoNodeException e) {
            LOG.info("{} ZNode did not exist", CURRENT_TSO_PATH);
        }

    }

    @AfterMethod
    public void afterMethod() {

        zkClient.close();

        CloseableUtils.closeQuietly(zkServer);
        zkServer = null;
        LOG.info("ZK Server Stopped");

    }

    @Test(timeOut = 30_000)
    public void testUnsuccessfulConnectionToTSO() throws Exception {

        // When no ZK node for TSOServer is found & no host:port config exists
        // we should get an exception when getting the client
        try {
            TSOClient.newInstance();
        } catch (IllegalArgumentException e) {
            // Expected
        }

    }

    @Test(timeOut = 30_000)
    public void testSuccessfulConnectionToTSOWithHostAndPort() throws Exception {

        // Launch a TSO WITHOUT publishing the address in ZK...
        String[] configArgs = new String[]{"-port", Integer.toString(tsoPortForTest), "-maxItems", "1000"};
        TSOServerCommandLineConfig tsoConfig = TSOServerCommandLineConfig.parseConfig(configArgs);
        injector = Guice.createInjector(new TSOMockModule(tsoConfig));
        LOG.info("Starting TSO");
        tsoServer = injector.getInstance(TSOServer.class);
        tsoServer.startAndWait();
        TestUtils.waitForSocketListening(TSO_HOST, tsoPortForTest, 100);
        LOG.info("Finished loading TSO");

        // When no ZK node for TSOServer is found we should get a connection
        // to the TSO through the host:port configured...
        OmidClientConfiguration tsoClientConf = OmidClientConfiguration.create();
        tsoClientConf.setConnectionString("localhost:" + tsoPortForTest);
        TSOClient tsoClient = TSOClient.newInstance(tsoClientConf);

        // ... so we should get responses from the methods
        Long startTS = tsoClient.getNewStartTimestamp().get();
        LOG.info("Start TS {} ", startTS);
        assertEquals(startTS.longValue(), 1);

        // Close the tsoClient connection and stop the TSO Server
        tsoClient.close().get();
        tsoServer.stopAndWait();
        tsoServer = null;
        TestUtils.waitForSocketNotListening(TSO_HOST, tsoPortForTest, 1000);
        LOG.info("TSO Server Stopped");

    }

    @Test(timeOut = 30_000)
    public void testSuccessfulConnectionToTSOThroughZK() throws Exception {

        // Launch a TSO publishing the address in ZK...
        String[] configArgs = new String[]{"-port", Integer.toString(tsoPortForTest), "-maxItems", "1000"};
        TSOServerCommandLineConfig config = TSOServerCommandLineConfig.parseConfig(configArgs);
        config.shouldHostAndPortBePublishedInZK = true;
        config.setLeasePeriodInMs(1000);
        config.setZKCluster(zkClusterForTest);
        injector = Guice.createInjector(new TSOMockModule(config));
        LOG.info("Starting TSO");
        tsoServer = injector.getInstance(TSOServer.class);
        tsoServer.startAndWait();
        TestUtils.waitForSocketListening(TSO_HOST, tsoPortForTest, 100);
        LOG.info("Finished loading TSO");

        waitTillTsoRegisters(injector.getInstance(CuratorFramework.class));

        // When a ZK node for TSOServer is found we should get a connection
        OmidClientConfiguration tsoClientConf = OmidClientConfiguration.create();
        tsoClientConf.setConnectionType(ZK);
        tsoClientConf.setConnectionString(zkClusterForTest);
        TSOClient tsoClient = TSOClient.newInstance(tsoClientConf);

        // ... so we should get responses from the methods
        Long startTS = tsoClient.getNewStartTimestamp().get();
        LOG.info("Start TS {} ", startTS);
        assertEquals(startTS.longValue(), 1);

        // Close the tsoClient connection and stop the TSO Server
        tsoClient.close().get();
        tsoServer.stopAndWait();
        tsoServer = null;
        TestUtils.waitForSocketNotListening(TSO_HOST, tsoPortForTest, 1000);
        LOG.info("TSO Server Stopped");

    }

    @Test(timeOut = 30_000)
    public void testSuccessOfTSOClientReconnectionsToARestartedTSOWithZKPublishing() throws Exception {

        // Start a TSO with ZK...
        String[] configArgs = new String[]{"-port", Integer.toString(tsoPortForTest), "-maxItems", "1000"};
        TSOServerCommandLineConfig tsoConfig = TSOServerCommandLineConfig.parseConfig(configArgs);
        tsoConfig.shouldHostAndPortBePublishedInZK = true;
        tsoConfig.setLeasePeriodInMs(1000);
        tsoConfig.setZKCluster(zkClusterForTest);
        injector = Guice.createInjector(new TSOMockModule(tsoConfig));
        LOG.info("Starting Initial TSO");
        tsoServer = injector.getInstance(TSOServer.class);
        tsoServer.startAndWait();
        TestUtils.waitForSocketListening(TSO_HOST, tsoPortForTest, 100);
        LOG.info("Finished loading TSO");

        waitTillTsoRegisters(injector.getInstance(CuratorFramework.class));

        // Then create the TSO Client under test...
        OmidClientConfiguration tsoClientConf = OmidClientConfiguration.create();
        tsoClientConf.setConnectionType(ZK);
        tsoClientConf.setConnectionString(zkClusterForTest);
        TSOClient tsoClient = TSOClient.newInstance(tsoClientConf);

        // ... and check that initially we get responses from the methods
        Long startTS = tsoClient.getNewStartTimestamp().get();
        LOG.info("Start TS {} ", startTS);
        assertEquals(startTS.longValue(), 1);

        // Then stop the server...
        tsoServer.stopAndWait();
        tsoServer = null;
        TestUtils.waitForSocketNotListening(TSO_HOST, tsoPortForTest, 1000);
        LOG.info("Initial TSO Server Stopped");

        // ... and check that we get a conn exception when trying to access the client
        try {
            startTS = tsoClient.getNewStartTimestamp().get();
            fail();
        } catch (ExecutionException e) {
            LOG.info("Exception expected");
            // Internal accessor to fsm to do the required checkings
            FsmImpl fsm = (FsmImpl) tsoClient.fsm;
            assertEquals(e.getCause().getClass(), ConnectionException.class);
            assertTrue(fsm.getState().getClass().equals(TSOClient.ConnectionFailedState.class)
                    ||
                    fsm.getState().getClass().equals(TSOClient.DisconnectedState.class));
        }

        // After that, simulate that a new TSO has been launched...
        Injector newInjector = Guice.createInjector(new TSOMockModule(tsoConfig));
        LOG.info("Re-Starting again the TSO");
        tsoServer = newInjector.getInstance(TSOServer.class);
        tsoServer.startAndWait();
        TestUtils.waitForSocketListening(TSO_HOST, tsoPortForTest, 100);
        LOG.info("Finished loading restarted TSO");

        // Finally re-check that, eventually, we can get a new value from the new TSO...
        boolean reconnectionActive = false;
        while (!reconnectionActive) {
            try {
                startTS = tsoClient.getNewStartTimestamp().get();
                reconnectionActive = true;
            } catch (ExecutionException e) {
                // Expected
            }
        }
        assertNotNull(startTS);

        // ...and stop the server
        tsoServer.stopAndWait();
        TestUtils.waitForSocketNotListening(TSO_HOST, tsoPortForTest, 1000);
        LOG.info("Restarted TSO Server Stopped");
    }

    private void waitTillTsoRegisters(CuratorFramework zkClient) throws Exception {
        while (true) {
            try {
                Stat stat = zkClient.checkExists().forPath(CURRENT_TSO_PATH);
                if (stat == null) {
                    continue;
                }
                LOG.info("TSO registered in ZK with path {}={}", CURRENT_TSO_PATH, stat.toString());
                if (stat.toString().length() == 0) {
                    continue;
                }
                return;
            } catch (Exception e) {
                LOG.debug("TSO still has not registered yet, sleeping...", e);
                Thread.sleep(500);
            }
        }
    }

}
