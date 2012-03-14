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

import org.apache.hadoop.conf.Configuration;

/**
 * Specify the isolation level
 * @author maysam
 *
 */
public class IsolationLevel {
   static public boolean checkForReadWriteConflicts;
   static public boolean checkForWriteWriteConflicts;

   static {
      Configuration conf = OmidConfiguration.create();
      if (conf.get("tso.rwcheck") == null || conf.get("tso.wwcheck") == null) {
         System.out.println("ISOLATION ERROR: the isolation level parameters are not set: " + conf.get("tso.rwcheck") + " " + conf.get("tso.wwcheck"));
         System.exit(1);
      }
      checkForReadWriteConflicts = conf.getBoolean("tso.rwcheck", false);
      checkForWriteWriteConflicts = conf.getBoolean("tso.wwcheck", true);
      if (IsolationLevel.checkForReadWriteConflicts)
         System.out.println("ISOLATION: check for read-write conflicts");
      if (IsolationLevel.checkForWriteWriteConflicts)
         System.out.println("ISOLATION: check for write-write conflicts");
      if (!IsolationLevel.checkForReadWriteConflicts && !IsolationLevel.checkForWriteWriteConflicts) {
         System.out.println("ISOLATION ERROR: I do not know which version it is");
         System.exit(1);
      }
   }
}


