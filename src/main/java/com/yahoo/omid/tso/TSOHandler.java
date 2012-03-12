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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;

import com.yahoo.omid.tso.TSOSharedMessageBuffer.ReadingBuffer;
import com.yahoo.omid.tso.messages.AbortRequest;
import com.yahoo.omid.tso.messages.AbortedTransactionReport;
import com.yahoo.omid.tso.messages.CommitQueryRequest;
import com.yahoo.omid.tso.messages.CommitQueryResponse;
import com.yahoo.omid.tso.messages.CommitRequest;
import com.yahoo.omid.tso.messages.CommitResponse;
import com.yahoo.omid.tso.messages.CommittedTransactionReport;
import com.yahoo.omid.tso.messages.FullAbortReport;
import com.yahoo.omid.tso.messages.LargestDeletedTimestampReport;
import com.yahoo.omid.tso.messages.TimestampRequest;
import com.yahoo.omid.tso.persistence.LoggerAsyncCallback.AddRecordCallback;
import com.yahoo.omid.tso.persistence.LoggerException;
import com.yahoo.omid.tso.persistence.LoggerException.Code;
import com.yahoo.omid.tso.persistence.LoggerProtocol;

/**
 * ChannelHandler for the TSO Server
 * @author maysam
 *
 */
public class TSOHandler extends SimpleChannelHandler {

   private static final Log LOG = LogFactory.getLog(TSOHandler.class);

   /**
    * Bytes monitor
    */
   public static final AtomicInteger transferredBytes = new AtomicInteger();
//   public static int transferredBytes = 0;
   public static int abortCount = 0;
   public static int hitCount = 0;
   public static long queries = 0;

   /**
    * Channel Group
    */
   private ChannelGroup channelGroup = null;
   private static ChannelGroup clientChannels = new DefaultChannelGroup("clients");
   
   private Map<Channel, ReadingBuffer> messageBuffersMap = new HashMap<Channel, ReadingBuffer>();
   
   /**
    * Timestamp Oracle
    */
   private TimestampOracle timestampOracle = null;

   /**
    * The wrapper for the shared state of TSO
    */
   private TSOState sharedState;
   
   private FlushThread flushThread;
   private ScheduledExecutorService executor;
   private ScheduledFuture<?> flushFuture;

   /**
    * Constructor
    * @param channelGroup
    */
   public TSOHandler(ChannelGroup channelGroup, TSOState state) {
      this.channelGroup = channelGroup;
      this.timestampOracle = state.getSO();
      this.sharedState = state;
      this.flushThread = new FlushThread();
      this.executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
         @Override
         public Thread newThread(Runnable r) {
            Thread t = new Thread(Thread.currentThread().getThreadGroup(), r);
            t.setDaemon(true);
            t.setName("Flush Thread");
            return t;
         }
      });
      this.flushFuture = executor.schedule(flushThread, TSOState.FLUSH_TIMEOUT, TimeUnit.MILLISECONDS);
   }

   /**
    * Returns the number of transferred bytes
    * @return the number of transferred bytes
    */
   public static long getTransferredBytes() {
      return transferredBytes.longValue();
   }

   /**
    * If write of a message was not possible before, we can do it here
    */
   @Override
      public void channelInterestChanged(ChannelHandlerContext ctx,
            ChannelStateEvent e) {
      }

   public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
      channelGroup.add(ctx.getChannel());
   }

   /**
    * Handle receieved messages
    */
   @Override
   public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
      Object msg = e.getMessage();
      if (msg instanceof TimestampRequest) {
         // System.out.println("Receive TimestampRequest .......................");
         handle((TimestampRequest) msg, ctx);
         return;
      } else if (msg instanceof CommitRequest) {
         // System.out.println("Receive CommitRequest .......................");
         handle((CommitRequest) msg, ctx);
         return;
      } else if (msg instanceof FullAbortReport) {
         handle((FullAbortReport) msg, ctx);
         return;
      } else if (msg instanceof CommitQueryRequest) {
         handle((CommitQueryRequest) msg, ctx);
         return;
      }
   }

   public void handle(AbortRequest msg, ChannelHandlerContext ctx) {
      synchronized (sharedState) {
         DataOutputStream toWAL  = sharedState.toWAL;
         try {
            toWAL.writeByte(LoggerProtocol.ABORT);
            toWAL.writeLong(msg.startTimestamp);
         } catch (IOException e) {
            e.printStackTrace();
         }
         abortCount++;
         sharedState.processAbort(msg.startTimestamp);
         synchronized (sharedMsgBufLock) {
            queueHalfAbort(msg.startTimestamp);
        }
      }
   }

   /**
    * Handle the TimestampRequest message
    */
   public void handle(TimestampRequest msg, ChannelHandlerContext ctx) {
        long timestamp;
        synchronized (sharedState) {
            try {
                timestamp = timestampOracle.next(sharedState.toWAL);
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
        }

        ReadingBuffer buffer;
        synchronized (messageBuffersMap) {
            buffer = messageBuffersMap.get(ctx.getChannel());
            if (buffer == null) {
                synchronized (sharedState) {
                    synchronized (sharedMsgBufLock) {
                        Channel channel = ctx.getChannel();
                        channel.write(new CommittedTransactionReport(sharedState.latestStartTimestamp, sharedState.latestCommitTimestamp));
                        for (Long halfAborted : sharedState.hashmap.halfAborted) {
                           channel.write(new AbortedTransactionReport(halfAborted));
                        }
                        channel.write(new AbortedTransactionReport(sharedState.latestHalfAbortTimestamp));
                        channel.write(new FullAbortReport(sharedState.latestFullAbortTimestamp));
                        channel.write(new LargestDeletedTimestampReport(sharedState.largestDeletedTimestamp));
                        buffer = sharedState.sharedMessageBuffer.new ReadingBuffer(channel);
                        messageBuffersMap.put(channel, buffer);
                        channelGroup.add(channel);
                        clientChannels.add(channel);
                        LOG.warn("Channel connected: " + messageBuffersMap.size());
                    }
                }
            }
        }
        synchronized (sharedMsgBufLock) {
            sharedState.sharedMessageBuffer.writeTimestamp(timestamp);
            buffer.flush();
            sharedState.sharedMessageBuffer.rollBackTimestamp();
        }
   }
   
   ChannelBuffer cb = ChannelBuffers.buffer(10);

   private boolean finish;

   public static long waitTime = 0;
   public static long commitTime = 0;
   public static long checkTime = 0;
   
   private Object sharedMsgBufLock = new Object();
   private Object callbackLock = new Object();
   
   /**
    * Handle the CommitRequest message
    */
   public void handle(CommitRequest msg, ChannelHandlerContext ctx) {
      CommitResponse reply = new CommitResponse(msg.startTimestamp);
      long time = 0;//System.nanoTime();
      long timeAfter;
      ByteArrayOutputStream baos = sharedState.baos;
      DataOutputStream toWAL  = sharedState.toWAL;
      synchronized (sharedState) {
         timeAfter = 0;//System.nanoTime();
         waitTime += (timeAfter - time);
         time = timeAfter;
         //0. check if it should abort
         if (msg.startTimestamp < timestampOracle.first()) {
            reply.committed = false;
            LOG.warn("Aborting transaction after restarting TSO");
         } else if (msg.startTimestamp < sharedState.largestDeletedTimestamp) {
            // Too old
            reply.committed = false;//set as abort
//            LOG.warn("Too old starttimestamp: ST "+ msg.startTimestamp +" MAX " + sharedState.largestDeletedTimestamp);
         } else {
            //1. check the write-write conflicts
            for (RowKey r: msg.rows) {
               long value;
               value = sharedState.hashmap.get(r.getRow(), r.getTable(), r.hashCode());
               if (value != 0 && value > msg.startTimestamp) {
                  //System.out.println("Abort...............");
                  reply.committed = false;//set as abort
                  break;
               } else if (value == 0 && sharedState.largestDeletedTimestamp > msg.startTimestamp) {
                  //then it could have been committed after start timestamp but deleted by recycling
                  System.out.println("Old............... " + sharedState.largestDeletedTimestamp + " " + msg.startTimestamp);
                  reply.committed = false;//set as abort
                  break;
               }
            }
         }

         timeAfter = 0;//System.nanoTime();
         checkTime += (timeAfter - time);
         time = timeAfter;

         if (reply.committed) {
            //2. commit
            try {
              long commitTimestamp = timestampOracle.next(toWAL);
              sharedState.uncommited.commit(commitTimestamp);
              sharedState.uncommited.commit(msg.startTimestamp);
              reply.commitTimestamp = commitTimestamp;
              if (msg.rows.length > 0) {
                  if(LOG.isDebugEnabled()){
                      LOG.debug("Adding commit to WAL");
                  }
                  toWAL.writeByte(LoggerProtocol.COMMIT);
                  toWAL.writeLong(msg.startTimestamp);
                  toWAL.writeLong(commitTimestamp);
//                  toWAL.writeByte(msg.rows.length);
   
                  for (RowKey r: msg.rows) {
//                     toWAL.write(r.getRow(), 0, r.getRow().length);
                     sharedState.largestDeletedTimestamp = sharedState.hashmap.put(r.getRow(), 
                                                     r.getTable(), 
                                                     commitTimestamp, 
                                                     r.hashCode(), 
                                                     sharedState.largestDeletedTimestamp);
                  }

                  sharedState.processCommit(msg.startTimestamp, commitTimestamp);
                  if (sharedState.largestDeletedTimestamp > sharedState.previousLargestDeletedTimestamp) {
                     toWAL.writeByte(LoggerProtocol.LARGESTDELETEDTIMESTAMP);
                     toWAL.writeLong(sharedState.largestDeletedTimestamp);
                     Set<Long> toAbort = sharedState.uncommited.raiseLargestDeletedTransaction(sharedState.largestDeletedTimestamp);
//                     if (!toAbort.isEmpty()) {
//                         LOG.warn("Slow transactions after raising max: " + toAbort);
//                         System.out.println("largest deleted ts: " + sharedState.largestDeletedTimestamp);
//                     }
                     synchronized (sharedMsgBufLock) {
                        for (Long id : toAbort) {
                           sharedState.hashmap.setHalfAborted(id);
                           queueHalfAbort(id);
                        }
                        queueLargestIncrease(sharedState.largestDeletedTimestamp);
                     }
                     sharedState.previousLargestDeletedTimestamp = sharedState.largestDeletedTimestamp;
                  }
                  synchronized (sharedMsgBufLock) {
                      queueCommit(msg.startTimestamp, commitTimestamp);
                  }
               }
            } catch (IOException e) {
               e.printStackTrace();
            }
         } else { //add it to the aborted list
            abortCount++;
            try {
               toWAL.writeByte(LoggerProtocol.ABORT);
               toWAL.writeLong(msg.startTimestamp);
            } catch (IOException e) {
               e.printStackTrace();
            }
            sharedState.processAbort(msg.startTimestamp);
            
            synchronized (sharedMsgBufLock) {
                queueHalfAbort(msg.startTimestamp);
            }
         }
         
         TSOHandler.transferredBytes.incrementAndGet();

         timeAfter = 0;//System.nanoTime();
         commitTime += (timeAfter - time);
         time = timeAfter;

         ChannelandMessage cam = new ChannelandMessage(ctx, reply);

         sharedState.nextBatch.add(cam);
         if (sharedState.baos.size() >= TSOState.BATCH_SIZE) {
             if(LOG.isDebugEnabled()){
                 LOG.debug("Going to add record of size " + sharedState.baos.size());
             }
            //sharedState.lh.asyncAddEntry(baos.toByteArray(), this, sharedState.nextBatch);
            sharedState.addRecord(baos.toByteArray(), 
                                            new AddRecordCallback() {
                @Override
                public void addRecordComplete(int rc, Object ctx) {
                    if (rc != Code.OK) {
                        LOG.warn("Write failed: " + LoggerException.getMessage(rc));
                        
                    } else {
                        synchronized (callbackLock) {
                           @SuppressWarnings("unchecked")
                           ArrayList<ChannelandMessage> theBatch = (ArrayList<ChannelandMessage>) ctx;
                           for (ChannelandMessage cam : theBatch) {
                              Channels.write(cam.ctx, Channels.succeededFuture(cam.ctx.getChannel()), cam.msg);
                           }
                        }
                        
                    }
                }
            }, sharedState.nextBatch);
            sharedState.nextBatch = new ArrayList<ChannelandMessage>(sharedState.nextBatch.size() + 5);
            sharedState.baos.reset();
         }

      }

   }

   /**
    * Handle the CommitQueryRequest message
    */
   public void handle(CommitQueryRequest msg, ChannelHandlerContext ctx) {
      CommitQueryResponse reply = new CommitQueryResponse(msg.startTimestamp);
      reply.queryTimestamp = msg.queryTimestamp;
      synchronized (sharedState) {
         queries++;
         //1. check the write-write conflicts
         long value;
         value = sharedState.hashmap.getCommittedTimestamp(msg.queryTimestamp);
         if (value != 0) { //it exists
            reply.commitTimestamp = value;
            reply.committed = value < msg.startTimestamp;//set as abort
         }
         else if (sharedState.hashmap.isHalfAborted(msg.queryTimestamp))
            reply.committed = false;
         else if (sharedState.uncommited.isUncommited(msg.queryTimestamp))
            reply.committed = false;
         else 
             reply.retry = true;
//         else if (sharedState.largestDeletedTimestamp >= msg.queryTimestamp) 
//            reply.committed = true;
         // TODO retry needed? isnt it just fully aborted?
         
         ctx.getChannel().write(reply);

         // We send the message directly. If after a failure the state is inconsistent we'll detect it

      }
   }

   public void flush() {
      synchronized (sharedState) {
          if(LOG.isDebugEnabled()){
              LOG.debug("Adding record, size: " + sharedState.baos.size());
          }
         sharedState.addRecord(sharedState.baos.toByteArray(), new AddRecordCallback() {
             @Override
             public void addRecordComplete(int rc, Object ctx) {
                 if (rc != Code.OK) {
                     LOG.warn("Write failed: " + LoggerException.getMessage(rc));
                     
                 } else {
                     synchronized (callbackLock) {
                        @SuppressWarnings("unchecked")
                        ArrayList<ChannelandMessage> theBatch = (ArrayList<ChannelandMessage>) ctx;
                        for (ChannelandMessage cam : theBatch) {
                           Channels.write(cam.ctx, Channels.succeededFuture(cam.ctx.getChannel()), cam.msg);
                        }
                     }
                     
                 }
             }
         }, sharedState.nextBatch);
         sharedState.nextBatch = new ArrayList<ChannelandMessage>(sharedState.nextBatch.size() + 5);
         sharedState.baos.reset();
         if (flushFuture.cancel(false)) {
            flushFuture = executor.schedule(flushThread, TSOState.FLUSH_TIMEOUT, TimeUnit.MILLISECONDS);
         }
      }
   }

   public class FlushThread implements Runnable {
      @Override
      public void run() {
         if (finish) {
             return;
         }
         if (sharedState.nextBatch.size() > 0) {
            synchronized (sharedState) {
               if (sharedState.nextBatch.size() > 0) {
                   if(LOG.isDebugEnabled()){
                       LOG.debug("Flushing log batch.");
                   }
                  flush();
               }
            }
         }
         flushFuture = executor.schedule(flushThread, TSOState.FLUSH_TIMEOUT, TimeUnit.MILLISECONDS);
      }
   }
   
   private void queueCommit(long startTimestamp, long commitTimestamp) {
       sharedState.sharedMessageBuffer.writeCommit(startTimestamp, commitTimestamp);
   }
   
   private void queueHalfAbort(long startTimestamp) {
       sharedState.sharedMessageBuffer.writeHalfAbort(startTimestamp);
   }
   
   private void queueFullAbort(long startTimestamp) {
       sharedState.sharedMessageBuffer.writeFullAbort(startTimestamp);
   }
   
   private void queueLargestIncrease(long largestTimestamp) {
       sharedState.sharedMessageBuffer.writeLargestIncrease(largestTimestamp);
   }

   /**
    * Handle the FullAbortReport message
    */
   public void handle(FullAbortReport msg, ChannelHandlerContext ctx) {
      synchronized (sharedState) {
         DataOutputStream toWAL  = sharedState.toWAL;
         try {
            toWAL.writeByte(LoggerProtocol.FULLABORT);
            toWAL.writeLong(msg.startTimestamp);
         } catch (IOException e) {
            e.printStackTrace();
         }
         sharedState.processFullAbort(msg.startTimestamp);
      }
      synchronized (sharedMsgBufLock) {
         queueFullAbort(msg.startTimestamp);
      }
   }

   /*
    * Wrapper for Channel and Message
    */
   public static class ChannelandMessage {
      ChannelHandlerContext ctx;
      TSOMessage msg;
      ChannelandMessage(ChannelHandlerContext c, TSOMessage m) {
         ctx = c;
         msg = m;
      }
   }


   @Override
      public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
         LOG.warn("TSOHandler: Unexpected exception from downstream.", e.getCause());
         Channels.close(e.getChannel());
      }

    public void stop() {
        finish = true;
    }
   
   
}
