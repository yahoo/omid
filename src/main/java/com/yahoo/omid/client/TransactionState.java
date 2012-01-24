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

package com.yahoo.omid.client;

import java.util.HashSet;
import java.util.Set;

import com.yahoo.omid.tso.RowKey;

public class TransactionState {
   private long startTimestamp;
   private long commitTimestamp;
   private Set<RowKeyFamily> writtenRows;
   private Set<RowKey> readRows;
   
   public TSOClient tsoclient;

   TransactionState() {
      startTimestamp = 0;
      commitTimestamp = 0;
      this.writtenRows = new HashSet<RowKeyFamily>();
      this.readRows = new HashSet<RowKey>();
   }

   TransactionState(long startTimestamp, TSOClient client) {
      this();
      this.startTimestamp = startTimestamp;;
      this.commitTimestamp = 0;
      this.tsoclient = client;
   }

   long getStartTimestamp() {
      return startTimestamp;
   }
   
   long getCommitTimestamp() {
      return commitTimestamp;
   }

   void setCommitTimestamp(long commitTimestamp) {
      this.commitTimestamp = commitTimestamp;
   }

   RowKeyFamily[] getWrittenRows() {
      return writtenRows.toArray(new RowKeyFamily[0]);
   }

   void addWrittenRow(RowKeyFamily row) {
      writtenRows.add(row);
   }

   RowKey[] getReadRows() {
      return readRows.toArray(new RowKey[0]);
   }

   void addReadRow(RowKey row) {
      readRows.add(row);
   }

   public String toString() {
      return "Transaction-" + Long.toHexString(startTimestamp);
   }
}
