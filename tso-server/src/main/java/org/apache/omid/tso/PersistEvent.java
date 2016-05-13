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

import com.google.common.base.Objects;
import org.jboss.netty.channel.Channel;

public final class PersistEvent {

    private MonitoringContext monCtx;

    enum Type {
        TIMESTAMP, COMMIT, ABORT
    }

    private Type type = null;
    private Channel channel = null;

    private boolean isCommitRetry = false;
    private long startTimestamp = 0L;
    private long commitTimestamp = 0L;
    private long lowWatermark = 0L;

    void makePersistCommit(long startTimestamp, long commitTimestamp, Channel c, MonitoringContext monCtx) {

        this.type = Type.COMMIT;
        this.startTimestamp = startTimestamp;
        this.commitTimestamp = commitTimestamp;
        this.channel = c;
        this.monCtx = monCtx;

    }

    void makePersistAbort(long startTimestamp, boolean isCommitRetry, Channel c, MonitoringContext monCtx) {

        this.type = Type.ABORT;
        this.startTimestamp = startTimestamp;
        this.isCommitRetry = isCommitRetry;
        this.channel = c;
        this.monCtx = monCtx;

    }

    void makePersistTimestamp(long startTimestamp, Channel c, MonitoringContext monCtx) {

        this.type = Type.TIMESTAMP;
        this.startTimestamp = startTimestamp;
        this.channel = c;
        this.monCtx = monCtx;

    }

    MonitoringContext getMonCtx() {

        return monCtx;

    }

    Type getType() {

        return type;

    }

    Channel getChannel() {

        return channel;

    }

    boolean isCommitRetry() {

        return isCommitRetry;

    }

    long getStartTimestamp() {

        return startTimestamp;

    }

    long getCommitTimestamp() {

        return commitTimestamp;

    }

    long getLowWatermark() {

        return lowWatermark;

    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("type", type)
                .add("isCommitRetry", isCommitRetry)
                .add("ST", startTimestamp)
                .add("CT", commitTimestamp)
                .add("LWM", lowWatermark)
                .toString();
    }

}