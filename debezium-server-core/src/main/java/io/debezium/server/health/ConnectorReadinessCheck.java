/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.health;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Readiness;

import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.DebeziumStatus;

@Readiness
@ApplicationScoped
public class ConnectorReadinessCheck implements HealthCheck {

    @Inject
    Instance<DebeziumConnectorRegistry> registries;

    @Override
    public HealthCheckResponse call() {
        var ready = registries.stream()
                .flatMap(r -> r.engines().stream())
                .allMatch(e -> e.status().state() == DebeziumStatus.State.POLLING);

        return HealthCheckResponse.named("debezium")
                .status(ready)
                .build();
    }
}
