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


package com.yahoo.omid.tso;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import java.util.ArrayList;

public class Elder implements Comparable<Elder> {

    protected long startTimestamp;
    protected long commitTimestamp = -1;
    //i need this constructor only for search purposes, do not use it to store elders
    public Elder(long id) {
        this.startTimestamp = id;
    }
    public Elder(long id, long commitTimestamp) {
        this.startTimestamp = id;
        this.commitTimestamp = commitTimestamp;
    }
    public long getId() {
        return startTimestamp;
    }
    public long getCommitTimestamp() {
        assert(commitTimestamp != -1);//this could happen if it is not set by constructor
        return commitTimestamp;
    }
    public int compareTo(Elder e) {
        //be careful not to cast long to int (neg to pos complexity ...)
        long diff = this.getId() - e.getId();
        if (diff > 0) return 1;
        if (diff < 0) return -1;
        return 0;
    }
    public boolean equals(Object o) {
        if (o instanceof Elder) {
            return this.getId() == ((Elder)o).getId();
        }
        return false;
    }

    Integer hash = null;
    @Override
    public int hashCode() {
        if (hash != null)
            return hash;
        final int prime = 31;
        int h = 1;
        int id = (int)getId();
        h = prime * h + (int) (id ^ (id >>> 32));
        hash = h;
        return hash;
    }

    @Override
    public String toString() {
        return "Elder id=" + getId();
    }
}
