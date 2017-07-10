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
package org.apache.omid.tso;

import org.jboss.netty.channel.Channel;

import java.io.Closeable;

interface ReplyProcessor extends Closeable {

    /**
     * The each reply to a transactional operation for a client is contained in a batch. The batch must be ordered
     * before sending the replies in order to not to break snapshot isolation properties.
     *
     * @param batchSequence
     *            a batch sequence number, used to enforce order between replies
     * @param batch
     *            a batch containing the transaction operations
     */
    void manageResponsesBatch(long batchSequence, Batch batch);

    /**
     * Allows to send a commit response back to the client.
     *
     * @param startTimestamp
     *            the start timestamp representing the tx identifier that is going to receive the commit response
     * @param commitTimestamp
     *            the commit timestamp
     * @param channel
     *            the channel used to send the response back to the client
     */
    void sendCommitResponse(long startTimestamp, long commitTimestamp, Channel channel);

    /**
     * Allows to send an abort response back to the client.
     *
     * @param startTimestamp
     *            the start timestamp representing the tx identifier that is going to receive the abort response
     * @param channel
     *            the channel used to send the response back to the client
     */
    void sendAbortResponse(long startTimestamp, Channel channel);

    /**
     * Allow to send a timestamp response back to the client.
     *
     * @param startTimestamp
     *            the start timestamp to return that will represent the tx identifier for the created transaction
     * @param channel
     *            the channel used to send the response back to the client
     */

    void sendTimestampResponse(long startTimestamp, Channel channel);

    /**
     * Allow to send a fence response back to the client.
     *
     * @param tableID
     *            the table we are creating the fence for
     * @param fenceTimestamp
     *            the fence timestamp to return
     * @param channel
     *            the channel used to send the response back to the client
     */

    void sendFenceResponse(long tableID, long fenceTimestamp, Channel c);

}

