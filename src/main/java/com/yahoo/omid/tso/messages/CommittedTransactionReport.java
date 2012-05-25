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

package com.yahoo.omid.tso.messages;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.jboss.netty.buffer.ChannelBuffer;

import com.yahoo.omid.tso.TSOMessage;

/**
 * The message object for sending a commit request to TSO
 * @author maysam
 *
 */
public class CommittedTransactionReport implements TSOMessage {   
    /**
     * Starting timestamp
     */
    public long startTimestamp;
    public long commitTimestamp;
    private byte high;

    long lastStartTimestamp = 0;
    long lastCommitTimestamp = 0;

    public CommittedTransactionReport() {
    }

    public CommittedTransactionReport(byte high) {
        this.high = high;
    }

    public CommittedTransactionReport(long startTimestamp, long commitTimestamp) {
        this.startTimestamp = startTimestamp;
        this.commitTimestamp = commitTimestamp;
    }

    @Override
    public String toString() {
        return "Committed Transaction Report: T_s:" + startTimestamp + " T_c:" + commitTimestamp;
    }

    @Override
    public void readObject(ChannelBuffer aInputStream)
    throws IOException {

    if (high >= 0) {
        //           System.out.println("1 byte " + high);
        startTimestamp = lastStartTimestamp + high;
        commitTimestamp = lastCommitTimestamp + 1;
    } else if ((high & 0x40) == 0) {
        byte low = aInputStream.readByte();
        //           System.out.println("2 bytes " + high + " " + low);
        long startDiff = low;
        startDiff |= (high << 4) & 0x3f0;
        //           long commitDiff = (low & 0x0f);

        startTimestamp = lastStartTimestamp + startDiff;
        //            commitTimestamp = lastCommitTimestamp + commitDiff;
        commitTimestamp = lastCommitTimestamp + 1;
    } else {
        //           System.out.println("Else " + high);
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
    }

    @Override
    public void writeObject(DataOutputStream aOutputStream)
    throws IOException {
    aOutputStream.writeLong(startTimestamp);
    aOutputStream.writeLong(commitTimestamp);
    }

    @Override
    public void writeObject(ChannelBuffer buffer)
    {
        buffer.writeLong(startTimestamp);
        buffer.writeLong(commitTimestamp);
    }
}

