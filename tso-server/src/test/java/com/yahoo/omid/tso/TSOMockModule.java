package com.yahoo.omid.tso;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.InMemoryCommitTable;
import com.yahoo.omid.metrics.MetricsRegistry;
import com.yahoo.omid.metrics.NullMetricsProvider;
import com.yahoo.omid.timestamp.storage.TimestampStorage;
import com.yahoo.omid.tso.TimestampOracleImpl.InMemoryTimestampStorage;

import javax.inject.Named;
import javax.inject.Singleton;

import java.net.SocketException;
import java.net.UnknownHostException;

import static com.yahoo.omid.tso.TSOServer.TSO_HOST_AND_PORT_KEY;

public class TSOMockModule extends AbstractModule {

    private final TSOServerConfig config;

    public TSOMockModule(TSOServerConfig config) {
        this.config = config;
    }

    @Override
    protected void configure() {

        bind(TSOChannelHandler.class).in(Singleton.class);
        bind(TSOStateManager.class).to(TSOStateManagerImpl.class).in(Singleton.class);
        bind(CommitTable.class).to(InMemoryCommitTable.class).in(Singleton.class);
        bind(TimestampStorage.class).to(InMemoryTimestampStorage.class).in(Singleton.class);
        bind(TimestampOracle.class).to(PausableTimestampOracle.class).in(Singleton.class);
        bind(Panicker.class).to(MockPanicker.class).in(Singleton.class);

        install(config.getLeaseModule());
        install(new DisruptorModule());

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

}
