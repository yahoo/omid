/**
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
package com.yahoo.omid.tso;

import java.io.IOException;

public class VoidLeaseManager implements LeaseManagement {

    private final TSOChannelHandler tsoChannelHandler;
    private TSOStateManager stateManager;

    public VoidLeaseManager(TSOChannelHandler tsoChannelHandler, TSOStateManager stateManager) {
        this.tsoChannelHandler = tsoChannelHandler;
        this.stateManager = stateManager;
    }

    @Override
    public void startService() throws LeaseManagementException, InterruptedException {
        try {
            stateManager.reset();
            tsoChannelHandler.reconnect();
        } catch (IOException e) {
            throw new LeaseManagementException("Error initializing Lease Manager", e);
        }
    }

    @Override
    public void stopService() throws LeaseManagementException {
        tsoChannelHandler.closeConnection();
    }

    @Override
    public boolean stillInLeasePeriod() {
        // We should always return true
        return true;
    }

}
