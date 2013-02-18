/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved. 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. See accompanying LICENSE file.
 */

package com.yahoo.omid.tso;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.yahoo.omid.client.TSOClient;
import com.yahoo.omid.replication.ReadersAwareBuffer;
import com.yahoo.omid.transaction.TTable;

/**
 * Class for Throughput Monitoring
 *
 */
public class ThroughputMonitor extends Thread {
   private static final Log LOG = LogFactory.getLog(ThroughputMonitor.class);
   
   TSOState state;
   
   /**
    * Constructor
    */
   public ThroughputMonitor(TSOState state) {
      this.state = state;
   }
   
   @Override
   public void run() {
      if (!LOG.isTraceEnabled()) {
         return;
      }
      try {
         long oldCounter = TSOHandler.getTransferredBytes();
         long oldAbortCount = TSOHandler.abortCount;
         long startTime = System.currentTimeMillis();

         long oldQueries = TSOHandler.queries;
         for (;;) {
            Thread.sleep(10000);
            
            long endTime = System.currentTimeMillis();
            long newCounter = TSOHandler.getTransferredBytes();
            long newAbortCount = TSOHandler.abortCount;

            long newQueries = TSOHandler.queries;

            if (TSOPipelineFactory.bwhandler != null) {
                TSOPipelineFactory.bwhandler.measure();
            }
            LOG.trace(String.format("SERVER: %4.3f TPS, %4.6f Abort/s  "
                  + " Avg diff flu: %5.2f Rec Bytes/s: %5.2fMBs Sent Bytes/s: %5.2fMBs %d "
                  + "Queries: %d CurrentBuffers: %d",
                    (newCounter - oldCounter) / (float)(endTime - startTime) * 1000,
                    (newAbortCount - oldAbortCount) / (float)(endTime - startTime) * 1000,
                    0.0,
                    TSOPipelineFactory.bwhandler != null ? TSOPipelineFactory.bwhandler.getBytesReceivedPerSecond() / (double) (1024 * 1024) : 0,
                    TSOPipelineFactory.bwhandler != null ? TSOPipelineFactory.bwhandler.getBytesSentPerSecond() / (double) (1024 * 1024) : 0,
                    state.largestDeletedTimestamp,
                    newQueries - oldQueries,
                    ReadersAwareBuffer.nBuffers
                    )
              );
            
            oldCounter = newCounter;
            oldAbortCount = newAbortCount;
            startTime = endTime;

            oldQueries = newQueries;
         }
      } catch (InterruptedException e) {
         // Stop monitoring asked
         return;
      }
   }
}
