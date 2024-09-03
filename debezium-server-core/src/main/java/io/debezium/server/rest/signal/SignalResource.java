/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.rest.signal;

import jakarta.inject.Inject;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;

import io.debezium.engine.DebeziumEngine;
import io.debezium.server.DebeziumServer;
import io.debezium.server.DebeziumServerConfig;

@Path("/signals")
public class SignalResource {

    @Inject
    DebeziumServerConfig config;

    @Inject
    DebeziumServer server;

    @POST
    public Response post(@NotNull DSSignal dsSignal) {
        var signaler = server.getSignaler();
        if (signaler == null || !config.api().enabled()) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
        }

        var signal = toSignal(dsSignal);
        signaler.signal(signal);
        return Response.accepted().build();
    }

    private DebeziumEngine.Signal toSignal(DSSignal dsSignal) {
        return new DebeziumEngine.Signal(dsSignal.id(), dsSignal.type(), dsSignal.data(), dsSignal.additionalData());
    }

}
