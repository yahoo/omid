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
import java.util.Iterator;
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
import com.yahoo.omid.tso.messages.EldestUpdate;
import com.yahoo.omid.tso.messages.ReincarnationReport;
import com.yahoo.omid.tso.messages.FailedElderReport;
import com.yahoo.omid.tso.messages.LargestDeletedTimestampReport;
import com.yahoo.omid.tso.messages.TimestampRequest;
import com.yahoo.omid.tso.persistence.LoggerAsyncCallback.AddRecordCallback;
import com.yahoo.omid.tso.persistence.LoggerException;
import com.yahoo.omid.tso.persistence.LoggerException.Code;
import com.yahoo.omid.tso.persistence.LoggerProtocol;
import com.yahoo.omid.IsolationLevel;
import java.util.Arrays;
import java.util.HashSet;


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
            handle((TimestampRequest) msg, ctx);
            return;
        } else if (msg instanceof CommitRequest) {
            handle((CommitRequest) msg, ctx);
            return;
        } else if (msg instanceof FullAbortReport) {
            handle((FullAbortReport) msg, ctx);
            return;
        } else if (msg instanceof ReincarnationReport) {
            handle((ReincarnationReport) msg, ctx);
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
                synchronized (toWAL) {
                    toWAL.writeByte(LoggerProtocol.ABORT);
                    toWAL.writeLong(msg.startTimestamp);
                }
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
                synchronized (sharedState.toWAL) {
                    timestamp = timestampOracle.next(sharedState.toWAL);
                }
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
                        synchronized (sharedState.hashmap) {
                            for (Long halfAborted : sharedState.hashmap.halfAborted) {
                                channel.write(new AbortedTransactionReport(halfAborted));
                            }
                            for (Iterator<Elder> failedElders = sharedState.elders.failedEldersIterator(); failedElders.hasNext(); ) {
                                Elder fe = failedElders.next();
                                channel.write(new FailedElderReport(fe.getId(), fe.getCommitTimestamp()));
                            }
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

    /**
     * Handle the CommitRequest message
     */
    public void handle(CommitRequest msg, ChannelHandlerContext ctx) {
        CommitResponse reply = new CommitResponse(msg.startTimestamp);
        DataOutputStream toWAL  = sharedState.toWAL;
        reply.committed = true;
        //HashSet<Integer> lockedSet = new HashSet();

        if (IsolationLevel.checkForReadWriteConflicts) {
            for (RowKey r : msg.readRows)
                r.index = (r.hashCode() & 0x7FFFFFFF) % TSOState.MAX_ITEMS;
            Arrays.sort(msg.readRows);//for reads I just need atomic access and do not need to hold the locks
        }
        for (RowKey r : msg.writtenRows)
            r.index = (r.hashCode() & 0x7FFFFFFF) % TSOState.MAX_ITEMS;
        Arrays.sort(msg.writtenRows);//to avoid deadlocks
        //nosynchronized (sharedState) any more{
        //{
        //0. check if it sould abort
        if (msg.startTimestamp < timestampOracle.first()) {
            reply.committed = false;
            LOG.warn("Aborting transaction after restarting TSO");
        } else if (msg.startTimestamp < sharedState.largestDeletedTimestamp) {
            // Too old
            reply.committed = false;//set as abort
            LOG.warn("Too old starttimestamp: ST "+ msg.startTimestamp +" MAX " + sharedState.largestDeletedTimestamp);
        } else if (msg.writtenRows.length > 0) {
            //1. check the read-write conflicts
            //for reads just need atomic access, no need to hold the locks
            //do this check befor locking the write rows, otherwise in case of conflict we will face deadlocks
            if (IsolationLevel.checkForReadWriteConflicts)
                reply.committed = checkForConflictsIn(msg.readRows, msg.startTimestamp, reply.committed, false);
            //always lock writes, since gonna update them anyway
            int lastIndex = -1;
            for (RowKey r: msg.writtenRows) {
                if (lastIndex != r.index) { //lockedSet.add(r.index)) {//do not lock twice
                    lastIndex = r.index;
                    long tmaxForConflictChecking = sharedState.hashmap.lock(r.index);
                    if (tmaxForConflictChecking > msg.startTimestamp) {
                        reply.committed = false;
                        break;
                    }
                }
            }
            if (IsolationLevel.checkForWriteWriteConflicts)
                reply.committed = checkForConflictsIn(msg.writtenRows, msg.startTimestamp, reply.committed, true);
        }
        else reply.committed = true;

        //this variable allows avoid sync blocks by not accessing largestDeletedTimestamp
        long newmax = -1;
        long oldmax = -1;
        if (reply.committed) {
            //2. commit
            try {
                reply.commitTimestamp = msg.startTimestamp;//default: Tc=Ts for read-only
                if (msg.writtenRows.length > 0) {
                    Set<Long> toAbort = null;
                    //2.5 check the write-write conflicts to detect elders
                    if (!IsolationLevel.checkForWriteWriteConflicts)
                        checkForElders(reply, msg);
                    //The following steps must be synchronized
                    synchronized (sharedState) {
                        newmax = oldmax = sharedState.largestDeletedTimestamp;
                        //a) obtaining a commit timestamp
                        synchronized (toWAL) {
                            reply.commitTimestamp = timestampOracle.next(toWAL);
                            //the recovery procedure assumes that write to the WAL and obtaining the commit timestamp is performed atmically
                            toWAL.writeByte(LoggerProtocol.COMMIT);
                            toWAL.writeLong(msg.startTimestamp);
                            toWAL.writeLong(reply.commitTimestamp);//Tc is not necessary in theory, since we abort the in-progress txn after recovery, but it makes it easier for the recovery algorithm to bypass snapshotting
                            if (reply.rowsWithWriteWriteConflict != null && reply.rowsWithWriteWriteConflict.size() > 0) {//ww conflict
                                //TODO: merge it with COMMIT entry
                                toWAL.writeByte(LoggerProtocol.ELDER);
                                toWAL.writeLong(msg.startTimestamp);
                            }
                        }
                        //b) make sure that it will not be aborted concurrently
                        sharedState.uncommited.commit(reply.commitTimestamp);
                        sharedState.uncommited.commit(msg.startTimestamp);
                        //c) commit the transaction
                        //newmax = sharedState.hashmap.setCommitted(msg.startTimestamp, reply.commitTimestamp, newmax);
                        newmax = sharedState.processCommit(msg.startTimestamp, reply.commitTimestamp, newmax);
                        if (reply.rowsWithWriteWriteConflict != null && reply.rowsWithWriteWriteConflict.size() > 0) {//if it is supposed to be reincarnated, also map Tc to Tc just in case of a future query.
                            newmax = sharedState.hashmap.setCommitted(reply.commitTimestamp, reply.commitTimestamp, newmax);
                        }
                        //d) report the commit to the immdediate next txn
                        synchronized (sharedMsgBufLock) {
                            queueCommit(msg.startTimestamp, reply.commitTimestamp);
                        }
                        //e) report eldest if it is changed by this commit
                        reportEldestIfChanged(reply, msg);
                        //f) report Tmax if it is changed
                        if (newmax > oldmax) {//I caused a raise in Tmax
                            sharedState.largestDeletedTimestamp = newmax;
                            toAbort = sharedState.uncommited.raiseLargestDeletedTransaction(newmax);
                            if (!toAbort.isEmpty())
                                LOG.warn("Slow transactions after raising max: " + toAbort);
                            synchronized (sharedMsgBufLock) {
                                for (Long id : toAbort)
                                    queueHalfAbort(id);
                                queueLargestIncrease(sharedState.largestDeletedTimestamp);
                            }
                        }
                    }
                    //now do the rest out of sync block to allow more concurrency
                    if(LOG.isDebugEnabled()){
                        LOG.debug("Adding commit to WAL");
                    }
                    //TODO: should we be able to recover the writeset of failed elders?
                    if (newmax > oldmax) {//I caused a raise in Tmax
                        synchronized (toWAL) {
                            toWAL.writeByte(LoggerProtocol.LARGESTDELETEDTIMESTAMP);
                            toWAL.writeLong(newmax);
                        }
                        synchronized (sharedState.hashmap) {
                            for (Long id : toAbort)
                                sharedState.hashmap.setHalfAborted(id);
                        }
                        if (!IsolationLevel.checkForWriteWriteConflicts) {
                            Set<Elder> eldersToBeFailed = sharedState.elders.raiseLargestDeletedTransaction(newmax);
                            if (eldersToBeFailed != null && !eldersToBeFailed.isEmpty()) {
                                LOG.warn("failed elder transactions after raising max: " + eldersToBeFailed + " from " + oldmax + " to " + newmax);
                                synchronized (sharedMsgBufLock) {
                                    //report failedElders to the clients
                                    for (Elder elder : eldersToBeFailed)
                                        queueFailedElder(elder.getId(), elder.getCommitTimestamp());
                                }
                            }
                        }
                    }
                    for (RowKey r: msg.writtenRows)
                        sharedState.hashmap.put(r.getRow(), r.getTable(), reply.commitTimestamp, r.hashCode());

                } else {//at least clean uncommited list
                    synchronized (sharedState.hashmap) {
                        sharedState.uncommited.commit(msg.startTimestamp);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else { //add it to the aborted list
            abortCount++;
            try {
                synchronized (toWAL) {
                    toWAL.writeByte(LoggerProtocol.ABORT);
                    toWAL.writeLong(msg.startTimestamp);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            synchronized (sharedState.hashmap) {
                sharedState.processAbort(msg.startTimestamp);
            }
            synchronized (sharedMsgBufLock) {
                queueHalfAbort(msg.startTimestamp);
            }
        }

        //for reads just need atomic access, no need to hold the locks
        //do it for writes though
        int lastIndex = -1;
        for (RowKey r: msg.writtenRows)
            if (lastIndex != r.index) {//account for two puts with the same index
                sharedState.hashmap.unlock(r.index);
                lastIndex = r.index;
            }


        TSOHandler.transferredBytes.incrementAndGet();

        ChannelandMessage cam = new ChannelandMessage(ctx, reply);

        synchronized (sharedState) {
            sharedState.nextBatch.add(cam);
            if (sharedState.baos.size() >= TSOState.BATCH_SIZE) {
                if(LOG.isDebugEnabled()){
                    LOG.debug("Going to add record of size " + sharedState.baos.size());
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
            }
        }

    }

    //}

    boolean checkForConflictsIn(RowKey[] rows, long startTimestamp, boolean committed, boolean isAlreadyLocked) {
        if (!committed)//already aborted
            return committed;
        for (RowKey r: rows) {
            long value;
            if (isAlreadyLocked)
                value = sharedState.hashmap.get(r.getRow(), r.getTable(), r.hashCode());
            else//perform an atomic read that acquires the lock and releases it afterwards
                value = sharedState.hashmap.atomicget(r.getRow(), r.getTable(), r.hashCode(), r.index, startTimestamp);
            if (value != 0 && value > startTimestamp) {
                return false;//set as abort
            } else if (value == 0 && sharedState.largestDeletedTimestamp > startTimestamp) {
                //then it could have been committed after start timestamp but deleted by recycling
                LOG.warn("Old............... " + sharedState.largestDeletedTimestamp + " " + startTimestamp);
                return false;//set as abort
            } else if (value == -1) {//means that tmaxForConflictChecking > startTimestamp
                LOG.warn("Old....-1......... " + sharedState.largestDeletedTimestamp + " " + startTimestamp);
                return false;//set as abort
            }
        }
        return true;
    }

    //check for write-write conflicts
    void checkForElders(CommitResponse reply, CommitRequest msg) {
        for (RowKey r: msg.writtenRows) {
            long value;
            value = sharedState.hashmap.get(r.getRow(), r.getTable(), r.hashCode());
            if (value != 0 && value > msg.startTimestamp) {
                aWWconflictDetected(reply, msg, r);
            } else if (value == 0 && sharedState.largestDeletedTimestamp > msg.startTimestamp) {
                //then it could have been committed after start timestamp but deleted by recycling
                aWWconflictDetected(reply, msg, r);
            }
        }
    }

    void reportEldestIfChanged(CommitResponse reply, CommitRequest msg) {
        //2. add it to elders list
        if (reply.rowsWithWriteWriteConflict != null && reply.rowsWithWriteWriteConflict.size() > 0) {
            ArrayList<RowKey> rowsWithWriteWriteConflict = new ArrayList<RowKey>(reply.rowsWithWriteWriteConflict);
            sharedState.elders.addElder(msg.startTimestamp, reply.commitTimestamp, rowsWithWriteWriteConflict);
            if (sharedState.elders.isEldestChangedSinceLastProbe()) {
                LOG.warn("eldest is changed: " + msg.startTimestamp);
                synchronized (sharedMsgBufLock) {
                    queueEldestUpdate(sharedState.elders.getEldest());
                }
                synchronized (sharedState.toWAL) {
                    try {
                        sharedState.toWAL.writeByte(LoggerProtocol.ELDEST);
                        sharedState.toWAL.writeLong(msg.startTimestamp);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            else
                LOG.warn("eldest " + sharedState.elders.getEldest() + " isnt changed by ww " + msg.startTimestamp );
        }
    }

    //A write-write conflict is detected and the proper action is taken here
    void aWWconflictDetected(CommitResponse reply, CommitRequest msg, RowKey wwRow) {
        //Since we abort only for read-write conflicts, here we just keep track of elders (transactions with ww conflict) and tell them to reincarnate themselves by reinserting the items with ww conflict
        //1. add it to the reply to the lients
        if (reply.rowsWithWriteWriteConflict == null)
            //I do not know the size, so I create the longest needed
            reply.rowsWithWriteWriteConflict = new ArrayList<RowKey>(msg.writtenRows.length);
        reply.rowsWithWriteWriteConflict.add(wwRow);
    }

    /**
     * Handle the CommitQueryRequest message
     */
    public void handle(CommitQueryRequest msg, ChannelHandlerContext ctx) {
        CommitQueryResponse reply = new CommitQueryResponse(msg.startTimestamp);
        reply.queryTimestamp = msg.queryTimestamp;
        synchronized (sharedState) {
            queries++;
            long value;
            value = sharedState.hashmap.getCommittedTimestamp(msg.queryTimestamp);
            if (value != 0) { //it exists
                reply.commitTimestamp = value;
                reply.committed = value < msg.startTimestamp;//set as abort
            }
            else if (sharedState.largestDeletedTimestamp < msg.queryTimestamp) 
                reply.committed = false;
            else if (sharedState.hashmap.isHalfAborted(msg.queryTimestamp))
                reply.committed = false;
            else 
                reply.retry = true;
            //retry is show that we cannot distinguish two cases
            //1. Tc < Tmax
            //2. Ts < Tmax && aborted && Cleanedup is sent after we read the value but is received before this query is processed.

            ctx.getChannel().write(reply);
            // We send the message directly. If after a failure the state is inconsistent we'll detect it
        }
    }

    public void flush() {
        synchronized (sharedState) {
            if(LOG.isDebugEnabled()){
                LOG.debug("Adding record, size " + sharedState.baos.size());
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

    private void queueEldestUpdate(Elder eldest) {
        long startTimestamp = eldest == null ? -1 : eldest.getId();
        sharedState.sharedMessageBuffer.writeEldest(startTimestamp);
    }

    private void queueReincarnatdElder(long startTimestamp) {
        sharedState.sharedMessageBuffer.writeReincarnatedElder(startTimestamp);
    }

    private void queueFailedElder(long startTimestamp, long commitTimestamp) {
        sharedState.sharedMessageBuffer.writeFailedElder(startTimestamp, commitTimestamp);
    }

    private void queueFullAbort(long startTimestamp) {
        sharedState.sharedMessageBuffer.writeFullAbort(startTimestamp);
    }

    private void queueLargestIncrease(long largestTimestamp) {
        sharedState.sharedMessageBuffer.writeLargestIncrease(largestTimestamp);
    }

    /**
     * Handle the ReincarnationReport message
     */
    public void handle(ReincarnationReport msg, ChannelHandlerContext ctx) {
        synchronized (sharedState) {
            LOG.warn("reincarnated: " + msg.startTimestamp);
            boolean itWasFailed = sharedState.elders.reincarnateElder(msg.startTimestamp);
            if (itWasFailed) {
                LOG.warn("a failed elder is reincarnated: " + msg.startTimestamp);
                //tell the clients that the failed elder is reincarnated
                synchronized (sharedMsgBufLock) {
                    queueReincarnatdElder(msg.startTimestamp);
                }
            }
            if (sharedState.elders.isEldestChangedSinceLastProbe()) {
                LOG.warn("eldest is changed: " + msg.startTimestamp);
                synchronized (sharedMsgBufLock) {
                    queueEldestUpdate(sharedState.elders.getEldest());
                }
            }
            else
                LOG.warn("eldest " + sharedState.elders.getEldest() + " isnt changed by reincarnated " + msg.startTimestamp );
        }
        synchronized (sharedState.toWAL) {
            try {
                sharedState.toWAL.writeByte(LoggerProtocol.REINCARNATION);
                sharedState.toWAL.writeLong(msg.startTimestamp);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Handle the FullAbortReport message
     */
    public void handle(FullAbortReport msg, ChannelHandlerContext ctx) {
        synchronized (sharedState) {
            DataOutputStream toWAL  = sharedState.toWAL;
            try {
                synchronized (sharedState.toWAL) {
                    toWAL.writeByte(LoggerProtocol.FULLABORT);
                    toWAL.writeLong(msg.startTimestamp);
                }
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

    private Object sharedMsgBufLock = new Object();
    private Object callbackLock = new Object();

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        LOG.warn("TSOHandler: Unexpected exception from downstream.", e.getCause());
        e.getCause().printStackTrace();
        Channels.close(e.getChannel());
    }

    public void stop() {
        finish = true;
    }


}

