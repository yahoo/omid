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
package org.apache.omid.tso;

import java.io.IOException;

/**
 * Functionality of a service delivering monotonic increasing timestamps.
 */
public interface TimestampOracle {

    /**
     * Allows the initialization of the Timestamp Oracle service.
     * @throws IOException
     *          raised if a problem during initialization is shown.
     */
    void initialize() throws IOException;

    /**
     * Returns the next timestamp.
     */
    long next();

    /**
     * Returns the last timestamp assigned.
     */
    long getLast();

}