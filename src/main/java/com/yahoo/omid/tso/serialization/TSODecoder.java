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

package com.yahoo.omid.tso.serialization;

import java.io.EOFException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.io.HalfStoreFileReader;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

import com.yahoo.omid.tso.TSOMessage;
import com.yahoo.omid.tso.TSOSharedMessageBuffer;
import com.yahoo.omid.tso.messages.AbortRequest;
import com.yahoo.omid.tso.messages.AbortedTransactionReport;
import com.yahoo.omid.tso.messages.CommitQueryRequest;
import com.yahoo.omid.tso.messages.CommitQueryResponse;
import com.yahoo.omid.tso.messages.CommitRequest;
import com.yahoo.omid.tso.messages.CommitResponse;
import com.yahoo.omid.tso.messages.CommittedTransactionReport;
import com.yahoo.omid.tso.messages.FullAbortReport;
import com.yahoo.omid.tso.messages.FailedElderReport;
import com.yahoo.omid.tso.messages.EldestUpdate;
import com.yahoo.omid.tso.messages.ReincarnationReport;
import com.yahoo.omid.tso.messages.LargestDeletedTimestampReport;
import com.yahoo.omid.tso.messages.TimestampRequest;
import com.yahoo.omid.tso.messages.TimestampResponse;

public class TSODecoder extends FrameDecoder {
    private static final Log LOG = LogFactory.getLog(TSODecoder.class);

    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buf) throws Exception {
        // Mark the current buffer position before any reading
        // because the whole frame might not be in the buffer yet.
        // We will reset the buffer position to the marked position if
        // there's not enough bytes in the buffer.
        buf.markReaderIndex();

        // The performance with ostream wrapper was bad!!
        // DataInputStream ostream = new DataInputStream( new
        // ChannelBufferInputStream( buf ) );
        int slice = buf.readableBytes();
        slice = slice > 16 ? 16 : slice;
//        System.out.println("Decoding bytes: " + TSOSharedMessageBuffer.dumpHex(buf.slice(buf.readerIndex(), slice)));
        ChannelBuffer ostream = buf;
        TSOMessage msg;
        try {
            byte type = ostream.readByte();
            if (LOG.isTraceEnabled()) {
                LOG.trace("Decoding message : " + type);
            }
            if ((type & 0xC0) == 0x00) {
               return readCommittedTransactionReport(type, ostream);
            } else if ((type & 0xC0) == 0x40) {
               return readAbortTransactionReport(type, ostream);
            } else if ((type & 0x40) == 0) {
               return readCommittedTransactionReport(type, ostream);
            } else if (type >= TSOMessage.CommittedTransactionReport) {
               return readCommittedTransactionReport(type, ostream);
            } else {
                switch (type) {
                case TSOMessage.TimestampRequest:
                    msg = new TimestampRequest();
                    break;
                case TSOMessage.TimestampResponse:
                    msg = new TimestampResponse();
                    break;
                case TSOMessage.CommitRequest:
                    msg = new CommitRequest();
                    break;
                case TSOMessage.CommitResponse:
                    msg = new CommitResponse();
                    break;
                case TSOMessage.FullAbortReport:
//                    msg = new FullAbortReport();
//                    break;
                case TSOMessage.FullAbortReportByte:
                    return readFullInteger(type, ostream);
//                    break;
                case TSOMessage.FailedElderReport:
                    msg = new FailedElderReport();
                    break;
                case TSOMessage.EldestUpdate:
                    msg = new EldestUpdate();
                    break;
                case TSOMessage.ReincarnationReport:
                    msg = new ReincarnationReport();
                    break;
                case TSOMessage.CommitQueryRequest:
                    msg = new CommitQueryRequest();
                    break;
                case TSOMessage.CommitQueryResponse:
                    msg = new CommitQueryResponse();
                    break;
                case TSOMessage.AbortedTransactionReport:
//                    msg = new AbortedTransactionReport();
//                    break;
                case TSOMessage.AbortedTransactionReportByte:
                    return readHalfInteger(type, ostream);
//                    break;
                case TSOMessage.CommittedTransactionReport:
                    msg = new CommittedTransactionReport();
                    break;
                case TSOMessage.LargestDeletedTimestampReport:
                    msg = new LargestDeletedTimestampReport();
                    break;
                case TSOMessage.AbortRequest:
                   msg = new AbortRequest();
                   break;
                default:
//                   System.out.println("Wrong type " + type); System.out.flush();
                    throw new Exception("Wrong type " + type + " " + ostream.toString().length());
                }
            }
            msg.readObject(ostream);
        } catch (IndexOutOfBoundsException e) {
            // Not enough byte in the buffer, reset to the start for the next try
            buf.resetReaderIndex();
            return null;
        } catch (EOFException e) {
            // Not enough byte in the buffer, reset to the start for the next try
            buf.resetReaderIndex();
            return null;
        }

        return msg;
    }

    private TSOMessage readAbortTransactionReport(byte type, ChannelBuffer ostream) {
      int diff = (((type & 0x1f) << 27) >> 27);
      TSOMessage msg;
      if ((type & 0x20) == 0) {
         // Half abort
         lastHalfAbortedTimestamp += diff;
         msg = new AbortedTransactionReport(lastHalfAbortedTimestamp);
      } else {
         // Full abort
         lastFullAbortedTimestamp += diff;
         msg =  new FullAbortReport(lastFullAbortedTimestamp);
      }
      return msg;
   }

   public long lastStartTimestamp = 0;
    long lastCommitTimestamp = 0;

    long lastHalfAbortedTimestamp = 0;
    long lastFullAbortedTimestamp = 0;
    
    private AbortedTransactionReport readHalfInteger(byte type, ChannelBuffer ostream) throws IOException {
        AbortedTransactionReport msg;
        if (type == TSOMessage.AbortedTransactionReport) {
            msg = new AbortedTransactionReport();
            msg.readObject(ostream);
        } else {
            msg = new AbortedTransactionReport();
            int diff = ostream.readByte();
            msg.startTimestamp = lastHalfAbortedTimestamp + diff;
        }
        lastHalfAbortedTimestamp = msg.startTimestamp;

        return msg;
    }
    
    private FullAbortReport readFullInteger(byte type, ChannelBuffer ostream) throws IOException {
        FullAbortReport msg;
        if (type == TSOMessage.FullAbortReport) {
            msg = new FullAbortReport();
            msg.readObject(ostream);
        } else {
            msg = new FullAbortReport();
            int diff = ostream.readByte();
            msg.startTimestamp = lastFullAbortedTimestamp + diff;
        }
        lastFullAbortedTimestamp = msg.startTimestamp;

        return msg;
    }

    private CommittedTransactionReport readCommittedTransactionReport(byte high, ChannelBuffer aInputStream) {
        long startTimestamp = 0;
        long commitTimestamp = 0;
        if (high >= 0) {
//            System.out.println("1 byte " + high);
            high = (byte) ((high << 26) >> 26);
            startTimestamp = lastStartTimestamp + high;
            commitTimestamp = lastCommitTimestamp + 1;
        } else if ((high & 0x40) == 0) {
            byte low = aInputStream.readByte();
//            System.out.println("2 bytes " + high + " " + low);
            long startDiff = low & 0xff;
            startDiff |= ((high & 0x3f) << 26) >> 18;
            // long commitDiff = (low & 0x0f);

//            startDiff = (startDiff << 50) >> 50;
            startTimestamp = lastStartTimestamp + startDiff;
            // commitTimestamp = lastCommitTimestamp + commitDiff;
            commitTimestamp = lastCommitTimestamp + 1;
        } else {
            // System.out.println("Else " + high);
            switch (high) {
            case TSOMessage.CommittedTransactionReportByteByte:
                startTimestamp = lastStartTimestamp + aInputStream.readByte();
                commitTimestamp = lastCommitTimestamp + aInputStream.readByte();
                break;
            case TSOMessage.CommittedTransactionReportShortByte:
                startTimestamp = lastStartTimestamp + aInputStream.readShort();
                commitTimestamp = lastCommitTimestamp + aInputStream.readByte();
                break;
            case TSOMessage.CommittedTransactionReportIntegerByte:
                startTimestamp = lastStartTimestamp + aInputStream.readInt();
                commitTimestamp = lastCommitTimestamp + aInputStream.readByte();
                break;
            case TSOMessage.CommittedTransactionReportLongByte:
                startTimestamp = lastStartTimestamp + aInputStream.readLong();
                commitTimestamp = lastCommitTimestamp + aInputStream.readByte();
                break;

            case TSOMessage.CommittedTransactionReportByteShort:
                startTimestamp = lastStartTimestamp + aInputStream.readByte();
                commitTimestamp = lastCommitTimestamp + aInputStream.readShort();
                break;
            case TSOMessage.CommittedTransactionReportShortShort:
                startTimestamp = lastStartTimestamp + aInputStream.readShort();
                commitTimestamp = lastCommitTimestamp + aInputStream.readShort();
                break;
            case TSOMessage.CommittedTransactionReportIntegerShort:
                startTimestamp = lastStartTimestamp + aInputStream.readInt();
                commitTimestamp = lastCommitTimestamp + aInputStream.readShort();
                break;
            case TSOMessage.CommittedTransactionReportLongShort:
                startTimestamp = lastStartTimestamp + aInputStream.readLong();
                commitTimestamp = lastCommitTimestamp + aInputStream.readShort();
                break;
            case TSOMessage.CommittedTransactionReport:
                startTimestamp = aInputStream.readLong();
                commitTimestamp = aInputStream.readLong();
            }
        }

        lastStartTimestamp = startTimestamp;
        lastCommitTimestamp = commitTimestamp;

        return new CommittedTransactionReport(startTimestamp, commitTimestamp);
    }

}
