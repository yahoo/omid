/**
 * Copyright 2011-2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.omid.tso;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.hbase.HBaseCommitTableConfig;
import com.yahoo.omid.timestamp.storage.TimestampStorage;
import com.yahoo.omid.tools.hbase.SecureHBaseConfig;

import javax.inject.Named;
import javax.inject.Singleton;

import java.net.SocketException;
import java.net.UnknownHostException;

import static com.yahoo.omid.tso.TSOServer.TSO_HOST_AND_PORT_KEY;

class TSOModule extends AbstractModule {
    private final TSOServerConfig config;

    TSOModule(TSOServerConfig config) {
        this.config = config;
    }

    @Override
    protected void configure() {

        bind(TSOChannelHandler.class).in(Singleton.class);
        bind(TSOStateManager.class).to(TSOStateManagerImpl.class).in(Singleton.class);
        bind(TimestampOracle.class).to(TimestampOracleImpl.class).in(Singleton.class);
        bind(Panicker.class).to(SystemExitPanicker.class).in(Singleton.class);

        // Disruptor setup
        install(new DisruptorModule());

    }

    @Provides
    TSOServerConfig provideTSOServerConfig() {
        return config;
    }

    @Provides
    @Named(TSO_HOST_AND_PORT_KEY)
    String provideTSOHostAndPort() throws SocketException, UnknownHostException {
        return NetworkInterfaceUtils.getTSOHostAndPort(config);

    }

}
