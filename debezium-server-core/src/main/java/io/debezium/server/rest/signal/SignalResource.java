/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.rest.signal;

import java.util.Optional;

import jakarta.inject.Inject;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;

import io.debezium.engine.DebeziumEngine;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumConnectorsRegistry;
import io.debezium.runtime.EngineManifest;
import io.debezium.server.configuration.DebeziumServerConfig;

@Path("/signals")
public class SignalResource {

    private final DebeziumServerConfig config;
    private final DebeziumConnectorsRegistry registry;

    @Inject
    public SignalResource(DebeziumServerConfig config, DebeziumConnectorsRegistry registry) {
        this.config = config;
        this.registry = registry;
    }

    @POST
    public Response post(DSSignal dsSignal) {
        if (!config.api().enabled()) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
        }

        var signaler = runningEngine()
                .map(Debezium::signaler)
                .orElse(null);

        if (signaler == null) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
        }

        signaler.signal(toSignal(dsSignal));
        return Response.accepted().build();
    }

    /**
     * Every Debezium Quarkus connector extension on the classpath declares an engine for the default
     * manifest, but Debezium Server only runs the one matching the configured {@code connector.class}.
     * Looking the engine up by manifest would therefore resolve an engine that never started, so the
     * running engines are the ones to search.
     *
     * @return the running engine, or empty when no engine is running yet
     */
    private Optional<Debezium> runningEngine() {
        return registry.runningEngines()
                .stream()
                .filter(engine -> EngineManifest.DEFAULT.equals(engine.manifest()))
                .findFirst();
    }

    private DebeziumEngine.Signal toSignal(DSSignal dsSignal) {
        return new DebeziumEngine.Signal(dsSignal.id(), dsSignal.type(), dsSignal.data(), dsSignal.additionalData());
    }

}
