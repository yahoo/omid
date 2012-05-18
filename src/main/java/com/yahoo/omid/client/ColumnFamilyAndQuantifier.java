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
                for (int i = 0; i < family.length; i++)
                    if (family[i] != other.family[i])
                        return false;
                for (int i = 0; i < qualifier.length; i++)
                    if (qualifier[i] != other.qualifier[i])
                        return false;
                return true;
            }
            return false;
        }
    @Override
        public int hashCode() {
            if (hash != null)
                return hash;
            int h = 0;
            h = computeHash(h, family);
            h = computeHash(h, qualifier);
            hash = h;
            return h;
        }
    private int computeHash(int hash, byte[] larray) {
        hash += larray.length;
        for (int i = 0; i < larray.length; i++) {
            hash += larray[i];
        }
        return hash;
    }
}
