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

import org.jboss.netty.buffer.ChannelBuffer;

import com.yahoo.omid.tso.RowKey;
import com.yahoo.omid.tso.TSOMessage;

/**
 * The message object for sending a commit request to TSO
 * @author maysam
 *
 */
public class CommitRequest implements TSOMessage {

    /**
     * Starting timestamp
     */
    public long startTimestamp;

    public CommitRequest() {
    }

    public CommitRequest(long startTimestamp) {
        this.startTimestamp = startTimestamp;
        this.writtenRows = new RowKey[0];
        this.readRows = new RowKey[0];
    }

    public CommitRequest(long startTimestamp, RowKey[] writtenRows) {
        this.startTimestamp = startTimestamp;
        this.writtenRows = writtenRows;
        this.readRows = new RowKey[0];
    }

    public CommitRequest(long startTimestamp, RowKey[] writtenRows, RowKey[] readRows) {
        this.startTimestamp = startTimestamp;
        this.writtenRows = writtenRows;
        this.readRows = readRows;
    }

    /**
     * Modified rows' ids
     */
    public RowKey[] writtenRows;
    public RowKey[] readRows;

    @Override
    public String toString() {
        return "CommitRequest: T_s:" + startTimestamp;
    }


    static private IndexOutOfBoundsException ex = new IndexOutOfBoundsException();
    @Override
    public void readObject(ChannelBuffer aInputStream)
    throws IOException {
    //      int totalSize = aInputStream.readInt();
//      if (totalSize < aInputStream.readableBytes()) {
//       throw ex;   
//      }
    long l = aInputStream.readLong();
    startTimestamp = l;
    //LOG.error("tid: " + startTimestamp + " capacity: " + aInputStream.capacity());
    int size = aInputStream.readInt();
    //      LOG.error("size: " + size);
    writtenRows = new RowKey[size];
    for (int i = 0; i < size; i++) {
        writtenRows[i] = RowKey.readObject(aInputStream);
    }
    size = aInputStream.readInt();
    readRows = new RowKey[size];
    for (int i = 0; i < size; i++) {
        readRows[i] = RowKey.readObject(aInputStream);
    }
    }

    @Override
    public void writeObject(ChannelBuffer buffer)  {
    }

    @Override
    public void writeObject(DataOutputStream aOutputStream)
    throws IOException {
    //       int size = 12;
//       for (RowKey r: rows) {
//           size += 2;
//           size += r.getRow().length;
//           size += r.getTable().length;
//        }
//       aOutputStream.writeInt(size);
    aOutputStream.writeLong(startTimestamp);
    aOutputStream.writeInt(writtenRows.length);
    for (RowKey r: writtenRows) {
        r.writeObject(aOutputStream);
    }
    aOutputStream.writeInt(readRows.length);
    for (RowKey r: readRows) {
        r.writeObject(aOutputStream);
    }
    }
}

