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
import java.util.HashSet;
import java.util.TreeSet;
import java.util.PriorityQueue;

public class Elders {
   
   //set is efficient for membership checking
   protected HashSet<Elder> setofelders;
   //heap is efficient for advancing largestDeletedTimestamp
   protected PriorityQueue<Elder> heapofelders;
   public Elders() {
      setofelders = new HashSet<Elder>();
      heapofelders = new PriorityQueue<Elder>();
   }

   public void addElder(long ts, long tc, RowKey[] wwRows) {
      Elder e = new Elder(ts, tc);
      //TODO: store the rest as well
      heapofelders.offer(e);
      setofelders.add(e);
      //System.out.println("WWWWWW " + ts);
   }
   
   public void reincarnateElder(long id) {
      Elder e = new Elder(id);
      //System.out.println("rrrrrr " + id);
      boolean isStillElder = setofelders.remove(e);
      if (isStillElder) {
         //System.out.println("RRRRRR " + id);
      }
      //else
      //System.out.println("nnnnn " + id);
      //do not do anything on heap
   }
   
   public Set<Elder> raiseLargestDeletedTransaction(long id) {
      Set<Elder> failed = new TreeSet<Elder>();
      while (heapofelders.size() > 0 && heapofelders.peek().getId() < id) {
         Elder e = heapofelders.poll();
      //System.out.println("mmmmm " + e.getId() + " < " + id);
      //System.out.flush();
         boolean isStillElder = setofelders.remove(e);
         if (isStillElder)
            failed.add(e);
      }
      return failed;
   }
}
