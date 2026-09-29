/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.rest.signal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;

import jakarta.ws.rs.core.Response;

import org.junit.jupiter.api.Test;

import io.debezium.engine.DebeziumEngine;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumConnectorsRegistry;
import io.debezium.runtime.EngineManifest;
import io.debezium.server.configuration.DebeziumServerConfig;

/**
 * Unit tests for {@link SignalResource}.
 *
 * <p>Debezium Server bundles one Debezium Quarkus connector extension per supported connector and each
 * of them declares an engine for {@link EngineManifest#DEFAULT}. Only the engine whose connector
 * matches the configured {@code connector.class} is started, so an engine found by manifest is not
 * necessarily an engine that can be signaled.
 */
public class SignalResourceTest {

    private static final DSSignal DS_SIGNAL = new DSSignal("1", "log", "{\"message\": \"Signal message\"}", null);
    private static final DebeziumEngine.Signal SIGNAL = new DebeziumEngine.Signal(DS_SIGNAL.id(), DS_SIGNAL.type(), DS_SIGNAL.data(),
            DS_SIGNAL.additionalData());

    @Test
    public void shouldSignalTheRunningEngine() {
        DebeziumEngine.Signaler signaler = mock(DebeziumEngine.Signaler.class);
        DebeziumConnectorsRegistry registry = registryWithRunningEngine(engine(signaler));

        SignalResource resource = new SignalResource(apiEnabled(true), registry);

        Response response = resource.post(DS_SIGNAL);

        assertThat(response.getStatus()).isEqualTo(Response.Status.ACCEPTED.getStatusCode());
        verify(signaler).signal(SIGNAL);
    }

    @Test
    public void shouldNotSignalAnEngineThatIsNotRunning() {
        DebeziumEngine.Signaler signaler = mock(DebeziumEngine.Signaler.class);
        DebeziumConnectorsRegistry registry = registryWithStoppedEngine(engine(signaler));

        SignalResource resource = new SignalResource(apiEnabled(true), registry);

        Response response = resource.post(DS_SIGNAL);

        assertThat(response.getStatus()).isEqualTo(Response.Status.SERVICE_UNAVAILABLE.getStatusCode());
        verifyNoInteractions(signaler);
    }

    @Test
    public void shouldReturnServiceUnavailableWhenNoEngineIsRunning() {
        DebeziumConnectorsRegistry registry = mock(DebeziumConnectorsRegistry.class);
        when(registry.runningEngines()).thenReturn(List.of());
        when(registry.get(EngineManifest.DEFAULT)).thenReturn(Optional.empty());

        SignalResource resource = new SignalResource(apiEnabled(true), registry);

        Response response = resource.post(DS_SIGNAL);

        assertThat(response.getStatus()).isEqualTo(Response.Status.SERVICE_UNAVAILABLE.getStatusCode());
    }

    @Test
    public void shouldReturnServiceUnavailableWhenApiIsDisabled() {
        DebeziumEngine.Signaler signaler = mock(DebeziumEngine.Signaler.class);
        DebeziumConnectorsRegistry registry = registryWithRunningEngine(engine(signaler));

        SignalResource resource = new SignalResource(apiEnabled(false), registry);

        Response response = resource.post(DS_SIGNAL);

        assertThat(response.getStatus()).isEqualTo(Response.Status.SERVICE_UNAVAILABLE.getStatusCode());
        verifyNoInteractions(signaler);
    }

    private DebeziumServerConfig apiEnabled(boolean enabled) {
        DebeziumServerConfig.Api api = mock(DebeziumServerConfig.Api.class);
        when(api.enabled()).thenReturn(enabled);

        DebeziumServerConfig config = mock(DebeziumServerConfig.class);
        when(config.api()).thenReturn(api);

        return config;
    }

    private DebeziumConnectorsRegistry registryWithRunningEngine(Debezium engine) {
        DebeziumConnectorsRegistry registry = mock(DebeziumConnectorsRegistry.class);
        when(registry.runningEngines()).thenReturn(List.of(engine));
        when(registry.get(EngineManifest.DEFAULT)).thenReturn(Optional.of(engine));

        return registry;
    }

    /**
     * An engine declared by a connector extension that the engine filter strategy did not select: it
     * is resolvable by manifest, but it never started and signaling it would go nowhere.
     */
    private DebeziumConnectorsRegistry registryWithStoppedEngine(Debezium engine) {
        DebeziumConnectorsRegistry registry = mock(DebeziumConnectorsRegistry.class);
        when(registry.runningEngines()).thenReturn(List.of());
        when(registry.get(EngineManifest.DEFAULT)).thenReturn(Optional.of(engine));

        return registry;
    }

    private Debezium engine(DebeziumEngine.Signaler signaler) {
        Debezium engine = mock(Debezium.class);
        when(engine.manifest()).thenReturn(EngineManifest.DEFAULT);
        when(engine.signaler()).thenReturn(signaler);

        return engine;
    }
}
