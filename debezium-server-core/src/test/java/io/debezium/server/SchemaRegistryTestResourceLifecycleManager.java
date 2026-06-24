/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import io.debezium.testing.testcontainers.testhelper.TestInfrastructureHelper;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import io.strimzi.test.container.StrimziKafkaCluster;

public class SchemaRegistryTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaRegistryTestResourceLifecycleManager.class);

    private static final Integer PORT = 8081;

    public static StrimziKafkaCluster KAFKA_CLUSTER = TestInfrastructureHelper.getKafkaCluster();

    private static final SchemaRegistryContainer schemaRegistryContainer = new SchemaRegistryContainer()
            .withNetwork(TestInfrastructureHelper.getNetwork())
            .withKafka(KAFKA_CLUSTER)
            .withLogConsumer(new Slf4jLogConsumer(LOGGER))
            .dependsOn(KAFKA_CLUSTER)
            .withStartupTimeout(Duration.ofSeconds(90));

    @Override
    public Map<String, String> start() {
        Startables.deepStart(Stream.of(KAFKA_CLUSTER, schemaRegistryContainer)).join();

        Map<String, String> params = new ConcurrentHashMap<>();
        params.put("debezium.format.schema.registry.url", getSchemaRegistryUrl());

        return params;
    }

    @Override
    public void stop() {
        try {
            if (schemaRegistryContainer != null) {
                schemaRegistryContainer.stop();
            }
        }
        catch (Exception e) {
            // ignored
        }
    }

    private static String getSchemaRegistryUrl() {
        return "http://" + schemaRegistryContainer.getHost() + ":" + schemaRegistryContainer.getMappedPort(PORT);
    }
}
