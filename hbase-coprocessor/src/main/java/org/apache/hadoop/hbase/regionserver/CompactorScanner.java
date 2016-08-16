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
package org.apache.hadoop.hbase.regionserver;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.apache.omid.HBaseShims;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.committable.CommitTable.Client;
import org.apache.omid.committable.CommitTable.CommitTimestamp;
import org.apache.omid.hbase.coprocessor.metrics.CompactorCoprocessorMetrics;
import org.apache.omid.transaction.CellUtils;
import org.apache.omid.transaction.CellInfo;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.SortedMap;
import java.util.concurrent.ExecutionException;

import static org.apache.omid.committable.CommitTable.CommitTimestamp.Location.SHADOW_CELL;

public class CompactorScanner implements InternalScanner {
    private static final Logger LOG = LoggerFactory.getLogger(CompactorScanner.class);
    private final InternalScanner internalScanner;
    private final CommitTable.Client commitTableClient;
    private final Queue<CommitTable.Client> commitTableClientQueue;
    private final boolean isMajorCompaction;
    private final boolean retainNonTransactionallyDeletedCells;
    private final long lowWatermark;

    private final Region hRegion;

    private boolean hasMoreRows = false;
    private List<Cell> currentRowWorthValues = new ArrayList<>();

    // Metrics
    private CompactorCoprocessorMetrics metrics;

    public CompactorScanner(ObserverContext<RegionCoprocessorEnvironment> e,
                            InternalScanner internalScanner,
                            Client commitTableClient,
                            Queue<CommitTable.Client> commitTableClientQueue,
                            boolean isMajorCompaction,
                            boolean preserveNonTransactionallyDeletedCells,
                            CompactorCoprocessorMetrics metrics) throws IOException
    {

        this.internalScanner = internalScanner;
        this.commitTableClient = commitTableClient;
        this.commitTableClientQueue = commitTableClientQueue;
        this.isMajorCompaction = isMajorCompaction;
        this.retainNonTransactionallyDeletedCells = preserveNonTransactionallyDeletedCells;
        this.lowWatermark = getLowWatermarkFromCommitTable();
        // Obtain the table in which the scanner is going to operate
        this.hRegion = HBaseShims.getRegionCoprocessorRegion(e.getEnvironment());
        this.metrics = metrics;
        LOG.info("Scanner cleaning up uncommitted txs older than LW [{}] in region [{}]",
                lowWatermark, hRegion.getRegionInfo());
    }

    @Override
    public boolean next(List<Cell> results) throws IOException {
        return next(results, -1);
    }

    public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
        int limit = scannerContext.getBatchLimit();
        return next(result, limit);
    }

    public boolean next(List<Cell> result, int limit) throws IOException {

        if (currentRowWorthValues.isEmpty()) {
            // 1) Read next row
            List<Cell> scanResult = new ArrayList<Cell>();
            hasMoreRows = internalScanner.next(scanResult);
            if (LOG.isTraceEnabled()) {
                LOG.trace("Row: Result {} limit {} more rows? {}", scanResult, limit, hasMoreRows);
            }
            if (hasMoreRows) {
                metrics.incrScannedRows();
            }
            // 2) Traverse result list separating normal cells from shadow
            // cells and building a map to access easily the shadow cells.
            SortedMap<Cell, Optional<Cell>> cellToSc = CellUtils.mapCellsToShadowCells(scanResult);

            // 3) traverse the list of row key values isolated before and
            // check which ones should be discarded
            Map<String, CellInfo> lastTimestampedCellsInRow = new HashMap<>();
            PeekingIterator<Map.Entry<Cell, Optional<Cell>>> iter
                    = Iterators.peekingIterator(cellToSc.entrySet().iterator());
            while (iter.hasNext()) {
                long cellProcessingTimeStartTimeInMs = System.currentTimeMillis();
                metrics.incrTotalCells();
                Map.Entry<Cell, Optional<Cell>> entry = iter.next();
                Cell cell = entry.getKey();
                Optional<Cell> shadowCellOp = entry.getValue();

                if (cell.getTimestamp() > lowWatermark) {
                    retain(currentRowWorthValues, cell, shadowCellOp);
                    metrics.updateCellProcessingTime(System.currentTimeMillis() - cellProcessingTimeStartTimeInMs);
                    continue;
                }

                if (shouldRetainNonTransactionallyDeletedCell(cell)) {
                    retain(currentRowWorthValues, cell, shadowCellOp);
                    metrics.updateCellProcessingTime(System.currentTimeMillis() - cellProcessingTimeStartTimeInMs);
                    continue;
                }

                // During a minor compaction the coprocessor may only see a
                // subset of store files and may not have the all the versions
                // of a cell available for consideration. Therefore, if it
                // deletes a cell with a tombstone during a minor compaction,
                // an older version of the cell may become visible again. So,
                // we have to remove tombstones only in major compactions.
                if (isMajorCompaction) {
                    if (CellUtils.isTombstone(cell)) {
                        if (shadowCellOp.isPresent()) {
                            skipToNextColumn(cell, iter);
                        } else {
                            Optional<CommitTimestamp> commitTimestamp = queryCommitTimestamp(cell);
                            // Clean the cell only if it is valid
                            if (commitTimestamp.isPresent() && commitTimestamp.get().isValid()) {
                                skipToNextColumn(cell, iter);
                            }
                        }
                        metrics.incrTombstoneCells();
                        metrics.updateCellProcessingTime(System.currentTimeMillis() - cellProcessingTimeStartTimeInMs);
                        continue;
                    }
                }

                if (shadowCellOp.isPresent()) {
                    saveLastTimestampedCell(lastTimestampedCellsInRow, cell, shadowCellOp.get());
                } else {
                    Optional<CommitTimestamp> commitTimestamp = queryCommitTimestamp(cell);
                    if (commitTimestamp.isPresent() && commitTimestamp.get().isValid()) {
                        // Build the missing shadow cell...
                        byte[] shadowCellValue = Bytes.toBytes(commitTimestamp.get().getValue());
                        Cell shadowCell = CellUtils.buildShadowCellFromCell(cell, shadowCellValue);
                        saveLastTimestampedCell(lastTimestampedCellsInRow, cell, shadowCell);
                        metrics.incrHealedShadowCells();
                    } else {
                        LOG.trace("Discarding cell {}", cell);
                        metrics.incrDiscardedCells();
                    }
                }
                metrics.updateCellProcessingTime(System.currentTimeMillis() - cellProcessingTimeStartTimeInMs);
            }
            retainLastTimestampedCellsSaved(currentRowWorthValues, lastTimestampedCellsInRow);
            metrics.incrRetainedCells(lastTimestampedCellsInRow.values().size());

            // 4) Sort the list
            Collections.sort(currentRowWorthValues, KeyValue.COMPARATOR);
        }

        // Chomp current row worth values up to the limit
        if (currentRowWorthValues.size() <= limit) {
            result.addAll(currentRowWorthValues);
            currentRowWorthValues.clear();
        } else {
            result.addAll(currentRowWorthValues.subList(0, limit));
            currentRowWorthValues.subList(0, limit).clear();
        }
        LOG.trace("Results to preserve {}", result);

        return hasMoreRows;
    }

    @Override
    public void close() throws IOException {
        internalScanner.close();
        commitTableClientQueue.add(commitTableClient);
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Helper methods
    // ----------------------------------------------------------------------------------------------------------------

    @VisibleForTesting
    public boolean shouldRetainNonTransactionallyDeletedCell(Cell cell) {
        return (CellUtil.isDelete(cell) || CellUtil.isDeleteFamily(cell))
                &&
                retainNonTransactionallyDeletedCells;
    }

    private void saveLastTimestampedCell(Map<String, CellInfo> lastCells, Cell cell, Cell shadowCell) {
        String cellKey = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength())
                + ":"
                + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
        LOG.trace("Cell Key: {}", cellKey);

        if (!lastCells.containsKey(cellKey)) {
            lastCells.put(cellKey, new CellInfo(cell, shadowCell));
        } else {
            if (lastCells.get(cellKey).getTimestamp() < cell.getTimestamp()) {
                lastCells.put(cellKey, new CellInfo(cell, shadowCell));
            } else {
                LOG.trace("Forgetting old cell {}", cell);
            }
        }
    }

    private long getLowWatermarkFromCommitTable() throws IOException {
        try {
            LOG.trace("About to read log watermark from commit table");
            return commitTableClient.readLowWatermark().get();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted getting low watermark from commit table", ie);
            throw new IOException("Interrupted getting low watermark from commit table");
        } catch (ExecutionException ee) {
            LOG.warn("Problem getting low watermark from commit table");
            throw new IOException("Problem getting low watermark from commit table", ee.getCause());
        }
    }

    private Optional<CommitTimestamp> queryCommitTimestamp(Cell cell) throws IOException {
        try {
            long queryCommitTableStartTimeInMs = System.currentTimeMillis();
            Optional<CommitTimestamp> ct = commitTableClient.getCommitTimestamp(cell.getTimestamp()).get();
            metrics.updateCommitTableQueryTime(System.currentTimeMillis() - queryCommitTableStartTimeInMs);
            if (ct.isPresent()) {
                return Optional.of(ct.get());
            } else {
                Get g = new Get(CellUtil.cloneRow(cell));
                byte[] family = CellUtil.cloneFamily(cell);
                byte[] qualifier = CellUtils.addShadowCellSuffix(cell.getQualifierArray(),
                        cell.getQualifierOffset(),
                        cell.getQualifierLength());
                g.addColumn(family, qualifier);
                g.setTimeStamp(cell.getTimestamp());
                Result r = hRegion.get(g);
                if (r.containsColumn(family, qualifier)) {
                    return Optional.of(new CommitTimestamp(SHADOW_CELL,
                            Bytes.toLong(r.getValue(family, qualifier)), true));
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while getting commit timestamp from commit table");
        } catch (ExecutionException e) {
            throw new IOException("Error getting commit timestamp from commit table", e);
        }

        return Optional.absent();
    }

    private void retain(List<Cell> result, Cell cell, Optional<Cell> shadowCell) {
        LOG.trace("Retaining cell {}", cell);
        result.add(cell);
        if (shadowCell.isPresent()) {
            LOG.trace("...with shadow cell {}", cell, shadowCell.get());
            result.add(shadowCell.get());
        } else {
            LOG.trace("...without shadow cell! (TS is above Low Watermark)");
        }
    }

    private void retainLastTimestampedCellsSaved(List<Cell> result, Map<String, CellInfo> lastTimestampedCellsInRow) {
        for (CellInfo cellInfo : lastTimestampedCellsInRow.values()) {
            LOG.trace("Retaining last cell {} with shadow cell {}", cellInfo.getCell(), cellInfo.getShadowCell());
            result.add(cellInfo.getCell());
            result.add(cellInfo.getShadowCell());
        }
    }

    private void skipToNextColumn(Cell cell, PeekingIterator<Map.Entry<Cell, Optional<Cell>>> iter) {

        int skippedCellsCount = 0;
        while (iter.hasNext()
                && CellUtil.matchingFamily(iter.peek().getKey(), cell)
                && CellUtil.matchingQualifier(iter.peek().getKey(), cell)) {
            iter.next();
            skippedCellsCount++;
        }
        metrics.incrSkippedCells(skippedCellsCount);

    }

}
