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

import java.util.Arrays;

//A wrapper for both column family and the qualifier
//Make it easier to be used in maps and hash maps
public class ColumnFamilyAndQuantifier {
    protected byte[] family;
    protected byte[] qualifier;
    protected Integer hash = null;
    ColumnFamilyAndQuantifier(byte[] f, byte[] q) {
        family = f;
        qualifier = q;
    }
    @Override
    public boolean equals(Object o) {
        if (o instanceof ColumnFamilyAndQuantifier) {
            ColumnFamilyAndQuantifier other = (ColumnFamilyAndQuantifier) o;
            if (family.length != other.family.length || qualifier.length != other.qualifier.length)
                return false;
            return (Arrays.equals(family, other.family) && Arrays.equals(qualifier, other.qualifier));
        }
        return false;
    }
    @Override
    public int hashCode() {
        if (hash != null)
            return hash;
        final int prime = 31;
        int h = 1;
        h = prime * h + Arrays.hashCode(family);
        h = prime * h + Arrays.hashCode(qualifier);
        hash = h;
        return h;
    }
}
