/**
 * Copyright 2011-2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.omid.tso;

import static com.codahale.metrics.MetricRegistry.name;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lmax.disruptor.WorkHandler;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.metrics.Histogram;
import com.yahoo.omid.metrics.MetricsRegistry;
import com.yahoo.omid.metrics.Timer;

public class PersistenceProcessorHandler implements WorkHandler<PersistenceProcessorImpl.PersistBatchEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(PersistenceProcessorHandler.class);

    private final String tsoHostAndPort;
    private final LeaseManagement leaseManager;

    final ReplyProcessor reply;
    final RetryProcessor retryProc;
    final CommitTable.Writer writer;
    final Panicker panicker;

    final int maxBatchSize;

    final Timer flushTimer;
    final Histogram batchSizeHistogram;

    @Inject
    PersistenceProcessorHandler(MetricsRegistry metrics,
            String tsoHostAndPort,
            LeaseManagement leaseManager,
            CommitTable commitTable,
            ReplyProcessor reply,
            RetryProcessor retryProc,
            Panicker panicker,
            TSOServerConfig config)
                    throws InterruptedException, ExecutionException, IOException {

        this.tsoHostAndPort = tsoHostAndPort;
        this.leaseManager = leaseManager;
        this.writer = commitTable.getWriter();
        this.reply = reply;
        this.retryProc = retryProc;
        this.panicker = panicker;
        this.maxBatchSize = config.getMaxBatchSize() / config.getPersistHandlerNum();

        LOG.info("Creating the persist processor handler with batch size {}",
                maxBatchSize);

        flushTimer = metrics.timer(name("tso", "persist", "flush"));
        batchSizeHistogram = metrics.histogram(name("tso", "persist", "batchsize"));
    }

    @Override
    public void onEvent(PersistenceProcessorImpl.PersistBatchEvent event) throws Exception {
        Batch batch = event.getBatch();
        for (int i=0; i < batch.getNumEvents(); ++i) {
            PersistEvent localEvent = batch.events[i];

            switch (localEvent.getType()) {
            case COMMIT:
                localEvent.getMonCtx().timerStart("commitPersistProcessor");
                // TODO: What happens when the IOException is thrown?
                writer.addCommittedTransaction(localEvent.getStartTimestamp(), localEvent.getCommitTimestamp());
                break;
            case ABORT:
                break;
            case TIMESTAMP:
                localEvent.getMonCtx().timerStart("timestampPersistProcessor");
                break;
            case LOW_WATERMARK:
                writer.updateLowWatermark(localEvent.getLowWatermark());
                break;
            default:
                assert(false);
            }
        }
        flush(batch, event.getBatchID());
    }

    private void flush(Batch batch, long batchID) throws IOException {

        long startFlushTimeInNs = System.nanoTime();

        boolean areWeStillMaster = true;
        if (!leaseManager.stillInLeasePeriod()) {
            // The master TSO replica has changed, so we must inform the
            // clients about it when sending the replies and avoid flushing
            // the current batch of TXs
            areWeStillMaster = false;
            // We need also to clear the data in the buffer
            writer.clearWriteBuffer();
            LOG.trace("Replica {} lost mastership before flushig data", tsoHostAndPort);
        } else {
            try {
                writer.flush();
            } catch (IOException e) {
                panicker.panic("Error persisting commit batch", e.getCause());
            }
            batchSizeHistogram.update(batch.getNumEvents());
            if (!leaseManager.stillInLeasePeriod()) {
                // If after flushing this TSO server is not the master
                // replica we need inform the client about it
                areWeStillMaster = false;
                LOG.warn("Replica {} lost mastership after flushig data", tsoHostAndPort);
            }
        }
        flushTimer.update((System.nanoTime() - startFlushTimeInNs));
        batch.sendReply(reply, retryProc, batchID, areWeStillMaster);
    }
}
