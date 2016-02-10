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
package com.yahoo.omid.metrics;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.util.List;

public class CodahaleModule extends AbstractModule {

    private static final Logger LOG = LoggerFactory.getLogger(CodahaleModule.class);

    private final List<String> metricsConfigs;

    public CodahaleModule(List<String> config) {
        this.metricsConfigs = config;
    }

    @Override
    protected void configure() {

    }

    @Provides @Singleton
    MetricsRegistry provideMetricsRegistry() {

        return CodahaleMetricsProvider.createCodahaleMetricsProvider(metricsConfigs);
        
    }

}
