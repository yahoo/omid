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

public class Elder implements Comparable<Elder> {
   
   protected long startTimestamp;
   public Elder(long id) {
      startTimestamp = id;
   }
   public long getId() {
      return startTimestamp;
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

   public int hashCode() {
      return (int)getId();
   }
}
