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

import org.apache.hadoop.hbase.metrics.BaseSource;

public interface CompactorCoprocessorMetricsSource extends BaseSource {

    /**
     * Update the compaction time histogram
     * @param timeInMs time it took
     */
    void updateCompactionTime(long timeInMs);

    /**
     * Update the time it took processing a cell
     * @param timeInMs time it took
     */
    void updateCellProcessingTime(long timeInMs);

    /**
     * Update the time it took to query the commit table for trying to find
     * the commit timestamp
     * @param timeInMs time it took
     */
    void updateCommitTableQueryTime(long timeInMs);

    /**
     * Increment the number of major compactions
     */
    public void incrMajorCompactions();

    /**
     * Increment the number of minor compactions
     */
    public void incrMinorCompactions();

    /**
     * Increment the number of scanned rows when compacting
     */
    void incrScannedRows();

    /**
     * Increment the number of total cells processed when compacting
     */
    void incrTotalCells();

    /**
     * Increment the number of retained cells when compacting
     * @param delta the delta to increment the counter
     */
    void incrRetainedCells(long delta);

    /**
     * Increment the number of skipped cells when compacting
     * @param delta the delta to increment the counter
     */
    void incrSkippedCells(long delta);

    /**
     * Increment the number of healed shadow cells when compacting
     */
    void incrHealedShadowCells();

    /**
     * Increment the number of discarded cells when compacting
     */
    void incrDiscardedCells();

    /**
     * Increment the number of tombstone cells when compacting
     */
    void incrTombstoneCells();

}
