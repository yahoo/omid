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
import org.apache.omid.tso.TSOMockModule;
import org.apache.omid.tso.TSOServer;
import org.apache.omid.tso.TSOServerConfig;
import org.apache.omid.tso.client.OmidClientConfiguration.ConflictDetectionLevel;
import org.apache.omid.tso.util.DummyCellIdImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestTSOClientRowAndCellLevelConflict {

    private static final Logger LOG = LoggerFactory.getLogger(TestTSOClientRowAndCellLevelConflict.class);

    private static final String TSO_SERVER_HOST = "localhost";
    private static final int TSO_SERVER_PORT = 5678;

    private OmidClientConfiguration tsoClientConf;

    // Required infrastructure for TSOClient test
    private TSOServer tsoServer;

    @BeforeMethod
    public void beforeMethod() throws Exception {

        TSOServerConfig tsoConfig = new TSOServerConfig();
        tsoConfig.setConflictMapSize(1000);
        tsoConfig.setPort(TSO_SERVER_PORT);
        tsoConfig.setNumConcurrentCTWriters(2);
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

        OmidClientConfiguration tsoClientConf = new OmidClientConfiguration();
        tsoClientConf.setConnectionString(TSO_SERVER_HOST + ":" + TSO_SERVER_PORT);

        this.tsoClientConf = tsoClientConf;

    }

    @AfterMethod
    public void afterMethod() throws Exception {
        tsoServer.stopAndWait();
        tsoServer = null;
        TestUtils.waitForSocketNotListening(TSO_SERVER_HOST, TSO_SERVER_PORT, 1000);
    }

    @Test(timeOut = 30_000)
    public void testRowLevelConflictAnalysisConflict() throws Exception {

        tsoClientConf.setConflictAnalysisLevel(ConflictDetectionLevel.ROW);

        TSOClient client = TSOClient.newInstance(tsoClientConf);

        CellId c1 = new DummyCellIdImpl(0xdeadbeefL, 0xdeadbeeeL);
        CellId c2 = new DummyCellIdImpl(0xfeedcafeL, 0xdeadbeeeL);

        Set<CellId> testWriteSet1 = Sets.newHashSet(c1);
        Set<CellId> testWriteSet2 = Sets.newHashSet(c2);
        
        long ts1 = client.getNewStartTimestamp().get();
        long ts2 = client.getNewStartTimestamp().get();
        
        client.commit(ts1, testWriteSet1).get();

        try {
            client.commit(ts2, testWriteSet2).get();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof AbortException, "Transaction should be aborted");
            return;
        }

        assertTrue(false, "Transaction should be aborted");
    }

    @Test(timeOut = 30_000)
    public void testRowLevelConflictAnalysisCommit() throws Exception {

        tsoClientConf.setConflictAnalysisLevel(ConflictDetectionLevel.ROW);

        TSOClient client = TSOClient.newInstance(tsoClientConf);

        CellId c1 = new DummyCellIdImpl(0xdeadbeefL, 0xdeadbeeeL);
        CellId c2 = new DummyCellIdImpl(0xfeedcafeL, 0xdeadbeefL);

        Set<CellId> testWriteSet1 = Sets.newHashSet(c1);
        Set<CellId> testWriteSet2 = Sets.newHashSet(c2);
        
        long ts1 = client.getNewStartTimestamp().get();
        long ts2 = client.getNewStartTimestamp().get();
        
        client.commit(ts1, testWriteSet1).get();

        try {
            client.commit(ts2, testWriteSet2).get();
        } catch (ExecutionException e) {
            assertFalse(e.getCause() instanceof AbortException, "Transaction should be committed");
            return;
        }

        assertTrue(true, "Transaction should be committed");
    }

    @Test(timeOut = 30_000)
    public void testCellLevelConflictAnalysisConflict() throws Exception {

        tsoClientConf.setConflictAnalysisLevel(ConflictDetectionLevel.CELL);

        TSOClient client = TSOClient.newInstance(tsoClientConf);

        CellId c1 = new DummyCellIdImpl(0xdeadbeefL, 0xdeadbeeeL);
        CellId c2 = new DummyCellIdImpl(0xdeadbeefL, 0xdeadbeeeL);

        Set<CellId> testWriteSet1 = Sets.newHashSet(c1);
        Set<CellId> testWriteSet2 = Sets.newHashSet(c2);
        
        long ts1 = client.getNewStartTimestamp().get();
        long ts2 = client.getNewStartTimestamp().get();
        
        client.commit(ts1, testWriteSet1).get();

        try {
            client.commit(ts2, testWriteSet2).get();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof AbortException, "Transaction should be aborted");
            return;
        }

        assertTrue(false, "Transaction should be aborted");
    }

    @Test(timeOut = 30_000)
    public void testCellLevelConflictAnalysisCommit() throws Exception {

        tsoClientConf.setConflictAnalysisLevel(ConflictDetectionLevel.CELL);

        TSOClient client = TSOClient.newInstance(tsoClientConf);

        CellId c1 = new DummyCellIdImpl(0xdeadbeefL, 0xdeadbeeeL);
        CellId c2 = new DummyCellIdImpl(0xfeedcafeL, 0xdeadbeefL);

        Set<CellId> testWriteSet1 = Sets.newHashSet(c1);
        Set<CellId> testWriteSet2 = Sets.newHashSet(c2);
        
        long ts1 = client.getNewStartTimestamp().get();
        long ts2 = client.getNewStartTimestamp().get();
        
        client.commit(ts1, testWriteSet1).get();

        try {
            client.commit(ts2, testWriteSet2).get();
        } catch (ExecutionException e) {
            assertFalse(e.getCause() instanceof AbortException, "Transaction should be committed");
            return;
        }

        assertTrue(true, "Transaction should be committed");
    }
    
}
