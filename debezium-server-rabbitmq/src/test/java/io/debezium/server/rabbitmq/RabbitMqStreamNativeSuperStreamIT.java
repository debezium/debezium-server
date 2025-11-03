/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

import jakarta.enterprise.event.Observes;

import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.OffsetSpecification;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.server.TestConfigSource;
import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;

/**
 * Integration test for RabbitMQ Stream Native Super Stream functionality.
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
@QuarkusTestResource(value = RabbitMqStreamTestResourceLifecycleManager.class)
@TestProfile(RabbitMqStreamNativeSuperStreamIT.TestProfile.class)
@EnabledIfSystemProperty(named = "debezium.sink.type", matches = "rabbitmqstream")
public class RabbitMqStreamNativeSuperStreamIT {

    private static final int MESSAGE_COUNT = 4;

    private static Environment environment;
    private static Consumer consumer;

    @ConfigProperty(name = "debezium.source.database.hostname")
    String dbHostname;

    @ConfigProperty(name = "debezium.source.database.port")
    String dbPort;

    @ConfigProperty(name = "debezium.source.database.user")
    String dbUser;

    @ConfigProperty(name = "debezium.source.database.password")
    String dbPassword;

    @ConfigProperty(name = "debezium.source.database.dbname")
    String dbName;

    private static final List<String> messages = Collections.synchronizedList(new ArrayList<>());

    {
        Testing.Files.delete(TestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(RabbitMqTestConfigSource.OFFSET_STORE_PATH);
    }

    void setupDependencies(@Observes ConnectorStartedEvent event) throws IOException, TimeoutException {
        // start consumer for super stream partitions
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RabbitMqStreamTestResourceLifecycleManager.container.getHost());
        factory.setPort(RabbitMqStreamTestResourceLifecycleManager.getPort());

        Address entryPoint = new Address(factory.getHost(), factory.getPort());
        environment = Environment.builder()
                .host(entryPoint.host())
                .port(entryPoint.port())
                .addressResolver(address -> entryPoint)
                .username(factory.getUsername())
                .password(factory.getPassword())
                .virtualHost(factory.getVirtualHost())
                .build();

        environment.streamCreator()
                .name(RabbitMqTestConfigSource.TOPIC_NAME)
                .superStream()
                .creator()
                .create();

        consumer = environment.consumerBuilder()
                .superStream(RabbitMqTestConfigSource.TOPIC_NAME)
                .offset(OffsetSpecification.first())
                .messageHandler((offset, message) -> {
                    messages.add(new String(message.getBodyAsBinary()));
                })
                .build();
    }

    @AfterAll
    static void stop() {
        if (consumer != null) {
            consumer.close();
        }
        if (environment != null) {
            environment.close();
        }
    }

    void connectorCompleted(@Observes ConnectorCompletedEvent event) throws Exception {
        if (!event.isSuccess()) {
            throw new RuntimeException(event.getError().get());
        }
    }

    @Test
    public void testRabbitMqStreamSuperStream() throws Exception {
        // consume initial records
        Awaitility.await()
                .atMost(Duration.ofSeconds(RabbitMqTestConfigSource.waitForSeconds()))
                .until(() -> messages.size() >= MESSAGE_COUNT);

        assertThat(messages.size()).isGreaterThanOrEqualTo(MESSAGE_COUNT);
        messages.clear();

        final JdbcConfiguration config = JdbcConfiguration.create()
                .with("hostname", dbHostname)
                .with("port", dbPort)
                .with("user", dbUser)
                .with("password", dbPassword)
                .with("dbname", dbName)
                .build();
        try (PostgresConnection connection = new PostgresConnection(config, "Debezium RabbitMQ Stream Super Stream Test")) {
            connection.execute(
                    "INSERT INTO inventory.customers VALUES (10000, 'John', 'Doe', 'jdoe@example.org')",
                    "DELETE FROM inventory.customers WHERE id=10000");
        }

        Awaitility.await()
                .atMost(Duration.ofSeconds(RabbitMqTestConfigSource.waitForSeconds()))
                .until(() -> messages.size() >= 2);

        assertThat(messages.size()).isGreaterThanOrEqualTo(2);
    }

    public static class TestProfile implements QuarkusTestProfile {
        @Override
        public java.util.Map<String, String> getConfigOverrides() {
            return java.util.Map.of(
                    "debezium.sink.type", "rabbitmqstream",
                    "debezium.sink.rabbitmqstream.superStream.enable", "true",
                    "debezium.sink.rabbitmqstream.producer.filterValue", "filter-not-found",
                    "debezium.sink.rabbitmqstream.superStream.partitions", "3");
        }
    }
}
