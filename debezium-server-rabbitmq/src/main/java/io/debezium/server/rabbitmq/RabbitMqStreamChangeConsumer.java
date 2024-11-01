/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.rabbitmq;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConnectionFactoryConfigurator;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.engine.Header;
import io.debezium.server.BaseChangeConsumer;

/**
 * Implementation of the consumer that delivers the messages into RabbitMQ Stream destination.
 *
 * @author Olivier Boudet
 *
 */
@Named("rabbitmq")
@Dependent
public class RabbitMqStreamChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqStreamChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.rabbitmq.";
    private static final String PROP_CONNECTION_PREFIX = PROP_PREFIX + "connection.";

    @ConfigProperty(name = PROP_PREFIX + "exchange", defaultValue = "")
    Optional<String> exchange;

    @ConfigProperty(name = PROP_PREFIX + "routingKey", defaultValue = "")
    Optional<String> routingKey;

    @ConfigProperty(name = PROP_PREFIX + "autoCreateRoutingKey", defaultValue = "false")
    Boolean autoCreateRoutingKey;

    @ConfigProperty(name = PROP_PREFIX + "routingKeyDurable", defaultValue = "true")
    Boolean routingKeyDurable;

    /**
     * When true, the routing key is calculated from topic name using stream name mapper.
     * When false the routingKey value or empty string is used.
     */
    @ConfigProperty(name = PROP_PREFIX + "routingKeyFromTopicName", defaultValue = "false")
    Boolean routingKeyFromTopicName;

    @ConfigProperty(name = PROP_PREFIX + "deliveryMode", defaultValue = "2")
    int deliveryMode;

    @ConfigProperty(name = PROP_PREFIX + "ackTimeout", defaultValue = "30000")
    int ackTimeout;

    @ConfigProperty(name = PROP_PREFIX + "null.value", defaultValue = "default")
    String nullValue;

    Connection connection;

    Channel channel;

    @PostConstruct
    void connect() {
        final Config config = ConfigProvider.getConfig();

        ConnectionFactory factory = new ConnectionFactory();
        Map<String, String> configProperties = getConfigSubset(config, PROP_CONNECTION_PREFIX).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> (entry.getValue() == null) ? null : entry.getValue().toString()));
        ConnectionFactoryConfigurator.load(factory, configProperties, "");

        LOGGER.info("Using connection to {}:{}", factory.getHost(), factory.getPort());

        try {
            connection = factory.newConnection();
            channel = connection.createChannel();
            channel.confirmSelect();

            if (!routingKeyFromTopicName && autoCreateRoutingKey) {
                final var routingKeyName = routingKey.orElse("");
                LOGGER.info("Creating queue for routing key named '{}'", routingKeyName);
                channel.queueDeclare(routingKeyName, routingKeyDurable, false, false, null);
            }
        }
        catch (IOException | TimeoutException e) {
            throw new DebeziumException(e);
        }
    }

    @PreDestroy
    void close() {

        try {
            if (channel != null) {
                channel.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
        catch (IOException | TimeoutException e) {
            throw new DebeziumException(e);
        }

    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        for (ChangeEvent<Object, Object> record : records) {
            LOGGER.trace("Received event '{}'", record);

            final var routingKeyName = routingKey
                    .orElse(routingKeyFromTopicName ? streamNameMapper.map(record.destination()) : "");
            final var exchangeName = exchange.orElse(streamNameMapper.map(record.destination()));

            try {
                if (routingKeyFromTopicName && autoCreateRoutingKey) {
                    LOGGER.trace("Creating queue for routing key named '{}'", routingKeyName);
                    channel.queueDeclare(routingKeyName, routingKeyDurable, false, false, null);
                }

                final Object value = (record.value() != null) ? record.value() : nullValue;
                channel.basicPublish(exchangeName, routingKeyName,
                        new AMQP.BasicProperties.Builder()
                                .deliveryMode(deliveryMode)
                                .headers(convertRabbitMqHeaders(record))
                                .build(),
                        getBytes(value));
            }
            catch (IOException e) {
                throw new DebeziumException(e);
            }
        }

        try {
            channel.waitForConfirmsOrDie(ackTimeout);
        }
        catch (IOException | TimeoutException e) {
            throw new DebeziumException(e);
        }

        LOGGER.trace("Marking {} records as processed.", records.size());
        for (ChangeEvent<Object, Object> record : records) {
            committer.markProcessed(record);
        }

        committer.markBatchFinished();
        LOGGER.trace("Batch marked finished");
    }

    private Map<String, Object> convertRabbitMqHeaders(ChangeEvent<Object, Object> record) {
        List<Header<Object>> headers = record.headers();
        Map<String, Object> rabbitMqHeaders = new HashMap<>();
        for (Header<Object> header : headers) {
            rabbitMqHeaders.put(header.getKey(), header.getValue());
        }
        return rabbitMqHeaders;
    }
}
