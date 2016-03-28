/**
 * Copyright 2011-2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.omid.tso;

import org.jboss.netty.channel.Channel;

public final class PersistEvent {

        private MonitoringContext monCtx;

        enum Type {
            TIMESTAMP, COMMIT, ABORT, LOW_WATERMARK
        }
        private Type type = null;
        private Channel channel = null;

        private boolean isRetry = false;
        private long startTimestamp = 0L;
        private long commitTimestamp = 0L;
        private long lowWatermark = 0L;

        void makePersistCommit(long startTimestamp, long commitTimestamp, Channel c,
                                      MonitoringContext monCtx) {
            this.type = Type.COMMIT;
            this.startTimestamp = startTimestamp;
            this.commitTimestamp = commitTimestamp;
            this.channel = c;
            this.monCtx = monCtx;
        }

        void makePersistAbort(long startTimestamp, boolean isRetry, Channel c,
                                     MonitoringContext monCtx) {
            this.type = Type.ABORT;
            this.startTimestamp = startTimestamp;
            this.isRetry = isRetry;
            this.channel = c;
            this.monCtx = monCtx;
        }

        void makePersistTimestamp(long startTimestamp, Channel c, MonitoringContext monCtx) {
            this.type = Type.TIMESTAMP;
            this.startTimestamp = startTimestamp;
            this.channel = c;
            this.monCtx = monCtx;
        }

        void makePersistLowWatermark(long lowWatermark, MonitoringContext monCtx) {
            this.type = Type.LOW_WATERMARK;
            this.lowWatermark = lowWatermark;
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

        boolean isRetry() {
            return isRetry;
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
    }