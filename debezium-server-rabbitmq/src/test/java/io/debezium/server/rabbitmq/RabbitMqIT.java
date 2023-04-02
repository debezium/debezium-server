/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

import jakarta.enterprise.event.Observes;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import io.debezium.server.TestConfigSource;
import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
@QuarkusTestResource(RabbitMqTestResourceLifecycleManager.class)
public class RabbitMqIT {

    private static final int MESSAGE_COUNT = 4;

    private static Connection connection;
    private static Channel channel = null;

    private static final List<String> messages = Collections.synchronizedList(new ArrayList<>());

    {
        Testing.Files.delete(TestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(RabbitMqTestConfigSource.OFFSET_STORE_PATH);
    }

    void setupDependencies(@Observes ConnectorStartedEvent event) throws IOException, TimeoutException {
        // start consumer
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RabbitMqTestResourceLifecycleManager.container.getHost());
        factory.setPort(RabbitMqTestResourceLifecycleManager.getPort());

        connection = factory.newConnection();
        channel = connection.createChannel();

        channel.exchangeDeclare(RabbitMqTestConfigSource.TOPIC_NAME, BuiltinExchangeType.DIRECT);
        String queue = channel.queueDeclare().getQueue();
        channel.queueBind(queue, RabbitMqTestConfigSource.TOPIC_NAME, "");

        channel.basicConsume(queue, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag,
                                       Envelope envelope,
                                       AMQP.BasicProperties properties,
                                       byte[] body) {
                String message = new String(body, StandardCharsets.UTF_8);
                messages.add(message);
            }
        });
    }

    @AfterAll
    static void stop() throws IOException, TimeoutException {
        if (channel != null) {
            channel.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    void connectorCompleted(@Observes ConnectorCompletedEvent event) throws Exception {
        if (!event.isSuccess()) {
            throw new RuntimeException(event.getError().get());
        }
    }

    @Test
    public void testRabbitMq() {

        // consume record
        Awaitility.await()
                .atMost(Duration.ofSeconds(RabbitMqTestConfigSource.waitForSeconds()))
                .until(() -> messages.size() >= MESSAGE_COUNT);

        assertThat(messages.size()).isGreaterThanOrEqualTo(MESSAGE_COUNT);
    }
}
