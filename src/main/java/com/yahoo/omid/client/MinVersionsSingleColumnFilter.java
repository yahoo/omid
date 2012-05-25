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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.*;

/**
 * Filter that sets both minTimestamp and minVersions
 * Assumes that there is only one column in the output
 * This filter is more optimized than MinVersionsFilter if
 * we know that the get asks for a single column qualifier
 * @maysam
 */
public class MinVersionsSingleColumnFilter extends FilterBase {

    //read at least minVersions and go till reach startTime
    long startTime = 0;
    long endTime = Long.MAX_VALUE;
    int minVersions;

    int includedVersions;


    /**
     * Used during deserialization. Do not use otherwise.
     */
    public MinVersionsSingleColumnFilter() {
        super();
    }

    public MinVersionsSingleColumnFilter(long startTime, long endTime, int minVersions) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.minVersions = minVersions;
        init();
    }

    private void init() {
        includedVersions = 0;
    }

    @Override
    public ReturnCode filterKeyValue(KeyValue v) {
        long version = v.getTimestamp();
        if (version >= endTime)
            return ReturnCode.SKIP;
        if (includedVersions < minVersions || version > startTime) {
            includedVersions++;
            return ReturnCode.INCLUDE;
        }
        return ReturnCode.NEXT_COL;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.startTime = in.readLong();
        this.endTime = in.readLong();
        this.minVersions = in.readInt();
        init();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.startTime);
        out.writeLong(this.endTime);
        out.writeInt(this.minVersions);
    }
}

