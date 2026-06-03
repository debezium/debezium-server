/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.producers;

import static io.debezium.config.CommonConnectorConfig.CONNECTOR_CLASS;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import io.debezium.runtime.Connector;
import io.debezium.runtime.DebeziumEngineFilterStrategy;
import io.debezium.runtime.configuration.DebeziumEngineRuntimeConfiguration;
import io.quarkus.arc.Unremovable;
import io.quarkus.runtime.Startup;

@ApplicationScoped
public class DebeziumEngineFilterStrategyProducer {

    private final DebeziumEngineRuntimeConfiguration configuration;

    @Inject
    public DebeziumEngineFilterStrategyProducer(DebeziumEngineRuntimeConfiguration configuration) {
        this.configuration = configuration;
    }

    @Startup
    @Produces
    @Unremovable
    @Singleton
    public DebeziumEngineFilterStrategy produce() {
        return debezium -> debezium.connector().equals(new Connector(configuration.defaultConfiguration().get(CONNECTOR_CLASS)));
    }
}
