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
   // The list of the failed elders: the elders that did not reincarnte in a timely manner
   protected TreeSet<Elder> failedElders;
   //the eldest of elders: the elder with min ts
   protected Elder eldest = null;
   protected boolean eldestChangedSinceLastProbe = false;
   //heap is efficient for advancing largestDeletedTimestamp
   //heap.peek is always valid but the other members might be stale
   protected PriorityQueue<Elder> heapofelders;
   public Elders() {
      setofelders = new HashSet<Elder>();
      heapofelders = new PriorityQueue<Elder>();
      failedElders = new TreeSet<Elder>();
   }

   public Elder getEldest() {
      return eldest;
   }

   public boolean isEldestChangedSinceLastProbe() {
      boolean res;
      synchronized (this) {
         res = eldestChangedSinceLastProbe;
         eldestChangedSinceLastProbe = false;
      }
      return res;
   }

   //check if the eldest is still eldest
   protected void updateEldest(Elder newElder) {
      assert(newElder != null);
      Elder oldEldest = eldest;
      if (eldest == null || eldest.getId() > newElder.getId())
         eldest = newElder;
      if (eldest != oldEldest)
         eldestChangedSinceLastProbe = true;
   }

   //the eldest is removed, elect new eldest
   protected void setEldest() {
      Elder oldEldest = eldest;
      //a failed elder is elder than a non-failed elder
      if (failedElders.size() > 0) {
         eldest = failedElders.first();//smallet
         return;
      }
      //then select eldest among normal elders
      //GC invalid peeks of the heap
      while (heapofelders.size() > 0 && !setofelders.contains(heapofelders.peek()))
         heapofelders.poll();
      if (heapofelders.size() == 0)
         eldest = null;
      else
         eldest = heapofelders.peek();
      if (eldest != oldEldest)
         eldestChangedSinceLastProbe = true;
   }

   public void addElder(long ts, long tc, RowKey[] wwRows) {
      synchronized (this) {
         Elder e = new Elder(ts, tc);
         //TODO: store the rest as well
         heapofelders.offer(e);
         setofelders.add(e);
         updateEldest(e);
         //System.out.println("WWWWWW " + ts);
      }
   }

   public boolean reincarnateElder(long id) {
      boolean itWasFailed;
      synchronized (this) {
         assert(eldest == null || eldest.getId() < id);
         Elder e = new Elder(id);
         boolean isStillElder = setofelders.remove(e);
         itWasFailed = false;
         if (!isStillElder)//then it is a failed elder
            itWasFailed = failedElders.remove(e);
         //do not do anything on heap
         if (eldest != null && eldest.getId() == id)
            setEldest();
      }
      return itWasFailed;
   }
   
   public Set<Elder> raiseLargestDeletedTransaction(long id) {
      Set<Elder> failed = null;
      synchronized (this) {
         while (heapofelders.size() > 0 && heapofelders.peek().getId() < id) {
            Elder e = heapofelders.poll();
            boolean isStillElder = setofelders.remove(e);
            if (isStillElder) {
               if (failed == null)
                  failed = new TreeSet<Elder>();
               failed.add(e);
            }
         }
      }
      return failed;
   }
}
