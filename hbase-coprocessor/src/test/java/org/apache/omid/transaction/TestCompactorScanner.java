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
package org.apache.omid.transaction;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.CompactorScanner;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.committable.CommitTable.Client;
import org.apache.omid.hbase.coprocessor.metrics.CompactorCoprocessorMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Queue;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestCompactorScanner {

    private static final Logger LOG = LoggerFactory.getLogger(TestCompactorScanner.class);

    private static final long TEST_TS = 1L;

    @DataProvider(name = "cell-retain-options")
    public Object[][] createCellRetainOptions() {
        return new Object[][]{
                {1, true}, {2, false},
        };
    }

    @Test(dataProvider = "cell-retain-options", timeOut = 60_000)
    public void testShouldRetainNonTransactionallyDeletedCellMethod(int optionIdx, boolean retainOption)
            throws Exception {

        // Create required mocks
        CompactorCoprocessorMetrics metrics = mock(CompactorCoprocessorMetrics.class);
        @SuppressWarnings("unchecked")
        ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
        InternalScanner internalScanner = mock(InternalScanner.class);
        CommitTable.Client ctClient = mock(CommitTable.Client.class);
        @SuppressWarnings("unchecked")
        Queue<Client> queue = mock(Queue.class);
        RegionCoprocessorEnvironment rce = mock(RegionCoprocessorEnvironment.class);
        HRegion hRegion = mock(HRegion.class);
        HRegionInfo hRegionInfo = mock(HRegionInfo.class);
        SettableFuture<Long> f = SettableFuture.create();

        // Wire required mock internals
        f.set(TEST_TS);
        when(ctClient.readLowWatermark()).thenReturn(f);
        when(ctx.getEnvironment()).thenReturn(rce);
        when(rce.getRegion()).thenReturn(hRegion);
        when(hRegion.getRegionInfo()).thenReturn(hRegionInfo);

        LOG.info("Testing when retain is {}", retainOption);
        try (CompactorScanner scanner = spy(new CompactorScanner(ctx,
                                                                 internalScanner,
                                                                 ctClient,
                                                                 queue,
                                                                 false,
                                                                 retainOption,
                                                                 metrics))) {

            // Different cell types to test
            KeyValue regularKV = new KeyValue(Bytes.toBytes("test-row"), TEST_TS, Type.Put);
            KeyValue deleteKV = new KeyValue(Bytes.toBytes("test-row"), TEST_TS, Type.Delete);
            KeyValue deleteColumnKV = new KeyValue(Bytes.toBytes("test-row"), TEST_TS, Type.DeleteColumn);
            KeyValue deleteFamilyKV = new KeyValue(Bytes.toBytes("test-row"), TEST_TS, Type.DeleteFamily);
            KeyValue deleteFamilyVersionKV = new KeyValue(Bytes.toBytes("test-row"), TEST_TS, Type.DeleteFamilyVersion);

            assertFalse(scanner.shouldRetainNonTransactionallyDeletedCell(regularKV));
            assertEquals(scanner.shouldRetainNonTransactionallyDeletedCell(deleteKV), retainOption);
            assertEquals(scanner.shouldRetainNonTransactionallyDeletedCell(deleteColumnKV), retainOption);
            assertEquals(scanner.shouldRetainNonTransactionallyDeletedCell(deleteFamilyKV), retainOption);
            assertEquals(scanner.shouldRetainNonTransactionallyDeletedCell(deleteFamilyVersionKV), retainOption);

        }

    }

}
