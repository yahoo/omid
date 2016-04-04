package com.yahoo.omid.transaction;

import com.google.inject.AbstractModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.hbase.HBaseCommitTable;
import com.yahoo.omid.metrics.MetricsRegistry;
import com.yahoo.omid.metrics.NullMetricsProvider;
import com.yahoo.omid.timestamp.storage.HBaseTimestampStorage;
import com.yahoo.omid.timestamp.storage.TimestampStorage;
import com.yahoo.omid.tso.DisruptorModule;
import com.yahoo.omid.tso.MockPanicker;
import com.yahoo.omid.tso.NetworkInterfaceUtils;
import com.yahoo.omid.tso.Panicker;
import com.yahoo.omid.tso.PausableTimestampOracle;
import com.yahoo.omid.tso.PersistenceProcessorHandler;
import com.yahoo.omid.tso.TSOChannelHandler;
import com.yahoo.omid.tso.TSOServerConfig;
import com.yahoo.omid.tso.TSOStateManager;
import com.yahoo.omid.tso.TSOStateManagerImpl;
import com.yahoo.omid.tso.TimestampOracle;

import org.apache.hadoop.conf.Configuration;

import javax.inject.Named;
import javax.inject.Singleton;

import java.net.SocketException;
import java.net.UnknownHostException;

import static com.yahoo.omid.tso.TSOServer.TSO_HOST_AND_PORT_KEY;

class TestTSOModule extends AbstractModule {

    private final Configuration hBaseConfig;
    private final TSOServerConfig config;

    TestTSOModule(Configuration hBaseConfig, TSOServerConfig config) {
        this.hBaseConfig = hBaseConfig;
        this.config = config;
    }

    @Override
    protected void configure() {

        bind(TSOChannelHandler.class).in(Singleton.class);

        bind(TSOStateManager.class).to(TSOStateManagerImpl.class).in(Singleton.class);

        bind(CommitTable.class).to(HBaseCommitTable.class).in(Singleton.class);
        bind(TimestampStorage.class).to(HBaseTimestampStorage.class).in(Singleton.class);
        bind(TimestampOracle.class).to(PausableTimestampOracle.class).in(Singleton.class);
        bind(Panicker.class).to(MockPanicker.class).in(Singleton.class);

        // Disruptor setup
        install(new DisruptorModule());

        // LeaseManagement setup
        install(config.getLeaseModule());
    }

    @Provides
    Configuration provideHBaseConfig() {
        return hBaseConfig;
    }

    @Provides
    TSOServerConfig provideTSOServerConfig() {
        return config;
    }

    @Provides
    @Singleton
    MetricsRegistry provideMetricsRegistry() {
        return new NullMetricsProvider();
    }

    @Provides
    @Named(TSO_HOST_AND_PORT_KEY)
    String provideTSOHostAndPort() throws SocketException, UnknownHostException {
        return NetworkInterfaceUtils.getTSOHostAndPort(config);
    }

    @Provides
    PersistenceProcessorHandler[] getPersistenceProcessorHandler(Provider<PersistenceProcessorHandler> provider) {
        PersistenceProcessorHandler[] persistenceProcessorHandlers = new PersistenceProcessorHandler[config.getPersistHandlerNum()];
        for (int i = 0; i < persistenceProcessorHandlers.length; i++) {
            persistenceProcessorHandlers[i] = provider.get();
        }
        return persistenceProcessorHandlers;
    }

}
