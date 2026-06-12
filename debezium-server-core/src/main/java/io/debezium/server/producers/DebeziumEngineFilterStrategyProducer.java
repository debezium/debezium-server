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

import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.debezium.runtime.Connector;
import io.debezium.runtime.DebeziumEngineFilterStrategy;
import io.debezium.runtime.configuration.DebeziumEngineRuntimeConfiguration;
import io.quarkus.arc.Unremovable;
import io.quarkus.runtime.Startup;

/**
 * CDI producer for {@link DebeziumEngineFilterStrategy}.
 *
 * <p>When {@code debezium.deployment.server} is {@code true}, Debezium Server runs in
 * retrocompatibility mode: it uses its own datasource configuration conventions instead
 * of Agroal. In this mode only a single datasource (and therefore a single engine) is
 * supported, so the produced strategy filters engines by matching the configured
 * {@code connector.class}.
 *
 * <p>When {@code debezium.deployment.server} is {@code false}, datasources are managed
 * by Agroal, which can identify multiple datasources and consequently multiple engines.
 * In that case the default (accept-all) strategy is returned.
 */
@ApplicationScoped
public class DebeziumEngineFilterStrategyProducer {

    private final DebeziumEngineRuntimeConfiguration configuration;

    @ConfigProperty(name = "debezium.deployment.server")
    boolean debeziumServerDeployment;

    @Inject
    public DebeziumEngineFilterStrategyProducer(DebeziumEngineRuntimeConfiguration configuration) {
        this.configuration = configuration;
    }

    /**
     * Produces a {@link DebeziumEngineFilterStrategy} based on the deployment mode.
     *
     * <p>In retrocompatibility mode ({@code debezium.deployment.server=true}), returns a
     * strategy that accepts only the engine whose {@link Connector} matches the configured
     * {@code connector.class}. Otherwise returns {@link DebeziumEngineFilterStrategy#DEFAULT}.
     *
     * @return the filter strategy instance
     */
    @Startup
    @Produces
    @Unremovable
    @Singleton
    public DebeziumEngineFilterStrategy produce() {
        if (!debeziumServerDeployment) {
            return DebeziumEngineFilterStrategy.DEFAULT;
        }

        return debezium -> debezium.connector().equals(new Connector(configuration.defaultConfiguration().get(CONNECTOR_CLASS)));
    }
}
