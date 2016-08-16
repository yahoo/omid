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
package org.apache.omid.hbase.coprocessor.metrics;

import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.omid.hbase.coprocessor.metrics.CompactorCoprocessorMetrics.DISCARDED_CELLS_KEY;
import static org.apache.omid.hbase.coprocessor.metrics.CompactorCoprocessorMetrics.HEALED_SHADOW_CELLS_KEY;
import static org.apache.omid.hbase.coprocessor.metrics.CompactorCoprocessorMetrics.MAJOR_COMPACTION_KEY;
import static org.apache.omid.hbase.coprocessor.metrics.CompactorCoprocessorMetrics.MINOR_COMPACTION_KEY;
import static org.apache.omid.hbase.coprocessor.metrics.CompactorCoprocessorMetrics.RETAINED_CELLS_KEY;
import static org.apache.omid.hbase.coprocessor.metrics.CompactorCoprocessorMetrics.SCANNED_ROWS_KEY;
import static org.apache.omid.hbase.coprocessor.metrics.CompactorCoprocessorMetrics.SKIPPED_CELLS_KEY;
import static org.apache.omid.hbase.coprocessor.metrics.CompactorCoprocessorMetrics.TOMBSTONE_CELLS_KEY;
import static org.apache.omid.hbase.coprocessor.metrics.CompactorCoprocessorMetrics.TOTAL_CELLS_KEY;

public class TestCompactorCoprocessorMetrics {

    public static MetricsAssertHelper HELPER = CompatibilityFactory.getInstance(MetricsAssertHelper.class);

    private CompactorCoprocessorMetrics compactorMetrics;

    @BeforeClass
    public static void classSetUp() {
        HELPER.init();
    }

    @BeforeMethod
    public void setUp() {
        compactorMetrics = new CompactorCoprocessorMetrics();
    }

    @Test
    public void testCounters() {

        for (int i = 0; i < 10; i++) {
            compactorMetrics.incrMajorCompactions();
        }
        HELPER.assertCounter(MAJOR_COMPACTION_KEY, 10, compactorMetrics);

        for (int i = 0; i < 11; i++) {
            compactorMetrics.incrMinorCompactions();
        }
        HELPER.assertCounter(MINOR_COMPACTION_KEY, 11, compactorMetrics);

        for (int i = 0; i < 12; i++) {
            compactorMetrics.incrScannedRows();
        }
        HELPER.assertCounter(SCANNED_ROWS_KEY, 12, compactorMetrics);

        for (int i = 0; i < 13; i++) {
            compactorMetrics.incrTotalCells();
        }
        HELPER.assertCounter(TOTAL_CELLS_KEY, 13, compactorMetrics);

        for (int i = 0; i < 14; i++) {
            compactorMetrics.incrRetainedCells(14);
        }
        HELPER.assertCounter(RETAINED_CELLS_KEY, 14 * 14, compactorMetrics);

        for (int i = 0; i < 15; i++) {
            compactorMetrics.incrSkippedCells(15);
        }
        HELPER.assertCounter(SKIPPED_CELLS_KEY, 15 * 15, compactorMetrics);

        for (int i = 0; i < 16; i++) {
            compactorMetrics.incrHealedShadowCells();
        }
        HELPER.assertCounter(HEALED_SHADOW_CELLS_KEY, 16, compactorMetrics);

        for (int i = 0; i < 17; i++) {
            compactorMetrics.incrDiscardedCells();
        }
        HELPER.assertCounter(DISCARDED_CELLS_KEY, 17, compactorMetrics);

        for (int i = 0; i < 18; i++) {
            compactorMetrics.incrTombstoneCells();
        }
        HELPER.assertCounter(TOMBSTONE_CELLS_KEY, 18, compactorMetrics);

    }

}
