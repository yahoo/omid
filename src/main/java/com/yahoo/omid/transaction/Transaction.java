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

package com.yahoo.omid.transaction;

/**
 * 
 * This class contains the required information to represent an Omid's
 * transaction, including the set of rows modified.
 * 
 */
public interface Transaction {
    /**
     * Modify the transaction associated with the current thread such that the
     * only possible outcome of the transaction is to roll back the transaction.
     */
    public void setRollbackOnly();
}
