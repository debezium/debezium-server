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
import org.eclipse.microprofile.health.Liveness;

import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.DebeziumStatus;

@Liveness
@ApplicationScoped
public class ConnectorLivenessCheck implements HealthCheck {

    @Inject
    Instance<DebeziumConnectorRegistry> registries;

    @Override
    public HealthCheckResponse call() {
        var live = registries.stream()
                .flatMap(r -> r.engines().stream())
                .anyMatch(e -> e.status().state() != DebeziumStatus.State.STOPPED);

        return HealthCheckResponse.named("debezium")
                .status(live)
                .build();
    }
}
