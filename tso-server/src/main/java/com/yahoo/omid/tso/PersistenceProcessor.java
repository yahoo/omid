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

interface PersistenceProcessor {
    void persistCommit(long startTimestamp, long commitTimestamp, Channel c, MonitoringContext monCtx);

    void persistAbort(long startTimestamp, boolean isRetry, Channel c, MonitoringContext monCtx);

    void persistTimestamp(long startTimestamp, Channel c, MonitoringContext monCtx);

    void persistLowWatermark(long lowWatermark, MonitoringContext monCtx);
    void persistFlush(boolean forTesting);
    void reset();
}
