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
package org.apache.omid.tso.client;

import java.util.List;
import java.util.Set;

import org.apache.omid.tso.client.OmidClientConfiguration.ConflictDetectionLevel;

/**
 * Defines the protocol used on the client side to abstract communication to the TSO server
 */
public interface TSOProtocol {

    /**
     * Returns a new timestamp assigned by on the server-side
     * @return the newly assigned timestamp as a future. If an error was detected, the future will contain a
     * corresponding protocol exception
     * see org.apache.omid.tso.TimestampOracle
     * see org.apache.omid.tso.TSOServer
     */
    TSOFuture<Long> getNewStartTimestamp();

    /**
     * Returns the result of the conflict detection made on the server-side for the specified transaction
     * @param transactionId
     *          the transaction to check for conflicts
     * @param writeSet
     *          the writeSet of the transaction, which includes all the modified cells
     * @return the commit timestamp as a future if the transaction was committed. If the transaction was aborted due
     * to conflicts with a concurrent transaction, the future will include an AbortException. If an error was detected,
     * the future will contain a corresponding protocol exception
     * see org.apache.omid.tso.TimestampOracle
     * see org.apache.omid.tso.TSOServer
     */
    TSOFuture<Long> commit(long transactionId, Set<? extends CellId> writeSet);

    /**
     * Returns a new fence timestamp assigned by on the server-side
     * @param tableId
     *          the table to create fence for.
     * @return the newly assigned timestamp as a future. If an error was detected, the future will contain a
     * corresponding protocol exception
     * see org.apache.omid.tso.TimestampOracle
     * see org.apache.omid.tso.TSOServer
     */
    TSOFuture<Long> getFence(long tableId);

    /**
     * Closes the communication with the TSO server
     * @return nothing. If an error was detected, the future will contain a corresponding protocol exception
     */
    TSOFuture<Void> close();

}
