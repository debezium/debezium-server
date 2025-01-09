/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.pravega;

import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import org.eclipse.microprofile.config.ConfigProvider;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import io.debezium.server.Images;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

/**
 * Runs a standalone Pravega cluster in-process.
 * <p>
 * <code>pravega.controller.uri</code> system property will contain the
 * Pravega Controller URI.
 */
public class PravegaTestResource implements QuarkusTestResourceLifecycleManager {

    public static final int CONTROLLER_PORT = 9090;
    public static final int SEGMENT_STORE_PORT = 12345;

    @SuppressWarnings("deprecation")
    private static final GenericContainer<?> container = new FixedHostPortGenericContainer<>(Images.PRAVEGA_IMAGE)
            .withFixedExposedPort(CONTROLLER_PORT, CONTROLLER_PORT)
            .withFixedExposedPort(SEGMENT_STORE_PORT, SEGMENT_STORE_PORT)
            .withStartupTimeout(Duration.ofSeconds(90))
            .waitingFor(Wait.forLogMessage(".*Pravega Sandbox is running locally now. You could access it at 127.0.0.1:9090.*", 1))
            .withCommand("standalone");

    @Override
    public Map<String, String> start() {
        container.start();

        String scope = ConfigProvider.getConfig().getValue("debezium.sink.pravega.scope", String.class);
        try (StreamManager streamManager = StreamManager.create(URI.create(getControllerUri()))) {
            streamManager.createScope(scope);
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.fixed(1))
                    .build();
            streamManager.createStream(scope, scope, streamConfig);
        }

        return Collections.singletonMap("pravega.controller.uri", getControllerUri());
    }

    @Override
    public void stop() {
        try {
            if (container != null) {
                container.stop();
            }
        }
        catch (Exception e) {
            // ignored
        }
    }

    public static String getControllerUri() {
        return "tcp://" + container.getHost() + ":" + container.getMappedPort(CONTROLLER_PORT);
    }

}
