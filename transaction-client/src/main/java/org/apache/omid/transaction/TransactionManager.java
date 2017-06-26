/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.omid.transaction;

import java.io.Closeable;

/**
 * Provides the methods to manage transactions (create, commit...)
 */
public interface TransactionManager extends Closeable {

    /**
     * Starts a new transaction.
     *
     * Creates and returns a {@link Transaction} interface implementation that will be used in TTable's methods for
     * doing operations on the transactional context defined by the returned object.
     *
     * @return transaction representation of the created transaction
     * @throws TransactionException in case of any issues
     */
    Transaction begin() throws TransactionException;

    /**
     * Commits a transaction.
     *
     * If the transaction was marked for rollback or has conflicts with another concurrent transaction it will be
     * rolledback automatically and a {@link RollbackException} will be thrown.
     *
     * @param tx transaction to be committed.
     * @throws RollbackException    thrown when transaction has conflicts with another transaction or when was marked
     *                              for rollback.
     * @throws TransactionException in case of any issues
     */
    void commit(Transaction tx) throws RollbackException, TransactionException;

    /**
     * Aborts a transaction.
     *
     * Automatically rollbacks the changes performed by the transaction.
     *
     * @param tx transaction to be rolled-back
     * @throws TransactionException  in case of any issues
     */
    void rollback(Transaction tx) throws TransactionException;

    /**
    * Creates a fence
    *
    * Creates a fence and returns a {@link Transaction} interface implementation that contains the fence information.
    *
    * @param tableName name of the table that requires a fence
    * @return transaction representation contains the fence timestamp as the TransactionId.
    * @throws TransactionException in case of any issues
    */
    Transaction fence(byte[] tableName) throws TransactionException;

}
