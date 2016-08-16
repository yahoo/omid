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

import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.lib.MetricMutableCounterLong;

public class CompactorCoprocessorMetrics extends BaseSourceImpl implements CompactorCoprocessorMetricsSource {

    private static final String METRICS_NAME = "CompactorCoprocessor";
    private static final String METRICS_CONTEXT = "omid.coprocessor.compactor";
    private static final String METRICS_DESCRIPTION = "Omid Compactor Coprocessor Metrics";
    private static final String METRICS_JMX_CONTEXT = "Omid,sub=" + METRICS_NAME;

    // ----------------------------------------------------------------------------------------------------------------
    // Metrics
    // ----------------------------------------------------------------------------------------------------------------

    // Histogram-related keys & descriptions
    static final String COMPACTIONS_KEY = "compactions";
    static final String COMPACTIONS_DESC = "Histogram about Compactions";
    static final String CELL_PROCESSING_KEY = "cellProcessing";
    static final String CELL_PROCESSING_DESC = "Histogram about Cell Processing";
    static final String COMMIT_TABLE_QUERY_KEY = "commitTableQuery";
    static final String COMMIT_TABLE_QUERY_DESC = "Histogram about Commit Table Query";

    // Counter-related keys & descriptions
    static final String MAJOR_COMPACTION_KEY = "major-compactions";
    static final String MAJOR_COMPACTION_DESC = "Number of major compactions";
    static final String MINOR_COMPACTION_KEY = "minor-compactions";
    static final String MINOR_COMPACTION_DESC = "Number of minor compactions";
    static final String SCANNED_ROWS_KEY = "scanned-rows";
    static final String SCANNED_ROWS_DESC = "Number of rows scanned";
    static final String TOTAL_CELLS_KEY = "total-cells";
    static final String TOTAL_CELLS_DESC = "Number of cells processed";
    static final String RETAINED_CELLS_KEY = "retained-cells";
    static final String RETAINED_CELLS_DESC = "Number of cells retained when compacting";
    static final String SKIPPED_CELLS_KEY = "skipped-cells";
    static final String SKIPPED_CELLS_DESC = "Number of cells skipped when compacting";
    static final String HEALED_SHADOW_CELLS_KEY = "healed-shadow-cells";
    static final String HEALED_SHADOW_CELLS_DESC = "Number of cells healed when compacting";
    static final String DISCARDED_CELLS_KEY = "discarded-cells";
    static final String DISCARDED_CELLS_DESC = "Number of cells discarded when compacting";
    static final String TOMBSTONE_CELLS_KEY = "tombstone-cells";
    static final String TOMBSTONE_CELLS_DESC = "Number of tombstone cells found when compacting";

    // *************************** Elements **********************************/

    // Histograms
    private final MetricHistogram compactionsHistogram;
    private final MetricHistogram cellProcessingHistogram;
    private final MetricHistogram commitTableQueryHistogram;

    // Counters
    private final MetricMutableCounterLong majorCompactionsCounter;
    private final MetricMutableCounterLong minorCompactionsCounter;
    private final MetricMutableCounterLong scannedRowsCounter;
    private final MetricMutableCounterLong totalCellsCounter;
    private final MetricMutableCounterLong retainedCellsCounter;
    private final MetricMutableCounterLong skippedCellsCounter;
    private final MetricMutableCounterLong healedShadowCellsCounter;
    private final MetricMutableCounterLong discardedCellsCounter;
    private final MetricMutableCounterLong tombstoneCellsCounter;

    // ----------------------------------------------------------------------------------------------------------------
    // End of Metrics
    // ----------------------------------------------------------------------------------------------------------------

    public CompactorCoprocessorMetrics() {
        this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
    }

    public CompactorCoprocessorMetrics(String metricsName,
                                       String metricsDescription,
                                       String metricsContext,
                                       String metricsJmxContext) {

        super(metricsName, metricsDescription, metricsContext, metricsJmxContext);

        // Histograms
        compactionsHistogram = getMetricsRegistry().newHistogram(COMPACTIONS_KEY, COMPACTIONS_DESC);
        cellProcessingHistogram = getMetricsRegistry().newHistogram(CELL_PROCESSING_KEY, CELL_PROCESSING_DESC);
        commitTableQueryHistogram = getMetricsRegistry().newHistogram(COMMIT_TABLE_QUERY_KEY, COMMIT_TABLE_QUERY_DESC);

        // Counters
        majorCompactionsCounter = getMetricsRegistry().newCounter(MAJOR_COMPACTION_KEY, MAJOR_COMPACTION_DESC, 0L);
        minorCompactionsCounter = getMetricsRegistry().newCounter(MINOR_COMPACTION_KEY, MINOR_COMPACTION_DESC, 0L);
        scannedRowsCounter = getMetricsRegistry().newCounter(SCANNED_ROWS_KEY, SCANNED_ROWS_DESC, 0L);
        totalCellsCounter = getMetricsRegistry().newCounter(TOTAL_CELLS_KEY, TOTAL_CELLS_DESC, 0L);
        retainedCellsCounter = getMetricsRegistry().newCounter(RETAINED_CELLS_KEY, RETAINED_CELLS_DESC, 0L);
        skippedCellsCounter = getMetricsRegistry().newCounter(SKIPPED_CELLS_KEY, SKIPPED_CELLS_DESC, 0L);
        healedShadowCellsCounter = getMetricsRegistry().newCounter(HEALED_SHADOW_CELLS_KEY, HEALED_SHADOW_CELLS_DESC, 0L);
        discardedCellsCounter = getMetricsRegistry().newCounter(DISCARDED_CELLS_KEY, DISCARDED_CELLS_DESC, 0L);
        tombstoneCellsCounter = getMetricsRegistry().newCounter(TOMBSTONE_CELLS_KEY, TOMBSTONE_CELLS_DESC, 0L);

    }

    // ----------------------------------------------------------------------------------------------------------------
    // CompactorCoprocessorMetricsSource Interface Impl
    // ----------------------------------------------------------------------------------------------------------------

    @Override
    public void updateCompactionTime(long timeInMs) {
        compactionsHistogram.add(timeInMs);
    }

    @Override
    public void updateCellProcessingTime(long timeInMs) {
        cellProcessingHistogram.add(timeInMs);
    }

    @Override
    public void updateCommitTableQueryTime(long timeInMs) {
        commitTableQueryHistogram.add(timeInMs);
    }

    @Override
    public void incrMajorCompactions() {
        majorCompactionsCounter.incr();
    }

    @Override
    public void incrMinorCompactions() {
        minorCompactionsCounter.incr();
    }

    @Override
    public void incrScannedRows() {
        scannedRowsCounter.incr();
    }

    @Override
    public void incrTotalCells() {
        totalCellsCounter.incr();
    }

    @Override
    public void incrRetainedCells(long delta) {
        retainedCellsCounter.incr(delta);
    }

    @Override
    public void incrSkippedCells(long delta) {
        skippedCellsCounter.incr(delta);
    }

    @Override
    public void incrHealedShadowCells() {
        healedShadowCellsCounter.incr();
    }

    @Override
    public void incrDiscardedCells() {
        discardedCellsCounter.incr();
    }

    @Override
    public void incrTombstoneCells() {
        tombstoneCellsCounter.incr();
    }

}
