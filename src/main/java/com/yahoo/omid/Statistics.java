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

package com.yahoo.omid;

//import java.io.DataOutputStream;
//import java.io.IOException;
import java.util.Map;
import java.util.EnumMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * To collect statistics
 * @author maysam
 *
 */
public class Statistics {
    private static final Log LOG = LogFactory.getLog(Statistics.class);
    public enum Tag {
        VSN_PER_HBASE_GET,//number of returned version per hbase get operation
            VSN_PER_CLIENT_GET,//number of returned version per client get operation
            GET_PER_CLIENT_GET,//number of hbase get performed per client get operation
            COMMIT,//number of commits
            REINCARNATION,//number of reincarnations
            ASKTSO,//number of queries sent to tso
            EMPTY_GET,//number of hbase get that return nothing
            dummy
    }
    static class History {
        public int cnt;
        public long total;
    }
    static Map<Tag, History> histories = new EnumMap<Tag, History>(Tag.class);
    static Map<Tag, History> partialChanges = new EnumMap<Tag, History>(Tag.class);
    static protected History getHistory(Tag tag, Map<Tag, History> map) {
        History history = map.get(tag);
        if (history == null) {
            history = new History();
            map.put(tag, history);
        }
        return history;
    }

    static public void partialReport(Tag tag, int value) {
        if(!LOG.isDebugEnabled())
            return;
        synchronized (histories) {
            History tmpHistory = getHistory(tag, partialChanges);
            tmpHistory.total += value;
        }
    }

    static public void partialReportOver(Tag tag) {
        if(!LOG.isDebugEnabled())
            return;
        synchronized (histories) {
            History tmpHistory = getHistory(tag, partialChanges);
            if (tmpHistory.total == 0)
                return;
            History history = getHistory(tag, histories);
            history.cnt ++;
            history.total += tmpHistory.total;
            tmpHistory.total = 0;
        }
    }

    static public void fullReport(Tag tag, int value) {
        if(!LOG.isDebugEnabled())
            return;
        synchronized (histories) {
            if (value == 0)
                return;
            History history = getHistory(tag, histories);
            history.cnt ++;
            history.total += value;
        }
    }

    static long lastReportTime = System.currentTimeMillis();
    static final long reportInterval = 2000;
    static private boolean skipReport() {
        long currentTimeMillis = System.currentTimeMillis();
        if (currentTimeMillis - lastReportTime > reportInterval) {
            lastReportTime = currentTimeMillis;
            return false;
        }
        return true;
    }
    static public void println() {
        if(!LOG.isDebugEnabled())
            return;
        synchronized (histories) {
            if (skipReport())
                return;
            StringBuffer tobelogged = new StringBuffer("Statistics: ");
            for (Map.Entry<Tag, History> entry : histories.entrySet()) {
                Tag tag = entry.getKey();
                History history = entry.getValue();
                tobelogged.append(tag + "Cnt " + history.cnt + " ");
                tobelogged.append(tag + "Sum " + history.total + " ");
                tobelogged.append(tag + "Avg " + (float) history.total / history.cnt + " ");
            }
            LOG.debug(tobelogged);
        }
    }
}

