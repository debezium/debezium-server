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
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConnectionFactoryConfigurator;

import io.debezium.DebeziumException;
import io.debezium.Module;
import io.debezium.annotation.VisibleForTesting;
import io.debezium.config.Field;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.engine.Header;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataFactory;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.DebeziumServerSink;
import io.debezium.server.StreamNameMapper;

/**
 * Implementation of the consumer that delivers the messages into RabbitMQ Stream destination.
 *
 * @author Olivier Boudet
 *
 */
@Named("rabbitmq")
@Dependent
public class RabbitMqStreamChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>>, DebeziumServerSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqStreamChangeConsumer.class);

    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();

    private static final String PROP_PREFIX = "debezium.sink.rabbitmq.";
    private static final String PROP_CONNECTION_PREFIX = PROP_PREFIX + "connection.";

    /**
     * Routing key is calculated from topic name using stream name mapper
     */
    private static final String TOPIC_ROUTING_KEY_SOURCE = "topic";

    /**
     * Routing key statically defined
     */
    private static final String STATIC_ROUTING_KEY_SOURCE = "static";

    /**
     * Routing key is the record key
     */
    private static final String KEY_ROUTING_KEY_SOURCE = "key";

    private static final String EMPTY_ROUTING_KEY = "";

    RabbitMqStreamChangeConsumerConfig config; // package-private for testing

    Connection connection;

    Channel channel;

    @PostConstruct
    void connect() {
        final Config mpConfig = ConfigProvider.getConfig();

        // Load configuration
        io.debezium.config.Configuration configuration = io.debezium.config.Configuration.from(getConfigSubset(mpConfig, PROP_PREFIX));
        this.config = new RabbitMqStreamChangeConsumerConfig(configuration);

        ConnectionFactory factory = new ConnectionFactory();
        Map<String, String> configProperties = getConfigSubset(mpConfig, PROP_CONNECTION_PREFIX).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> (entry.getValue() == null) ? null : entry.getValue().toString()));
        ConnectionFactoryConfigurator.load(factory, configProperties, "");

        LOGGER.info("Using connection to {}:{}", factory.getHost(), factory.getPort());

        if (config.isRoutingKeyFromTopicName()) {
            LOGGER.warn("Using deprecated `{}` config value. Please, use `{}` with value `topic` instead", PROP_PREFIX + "routingKeyFromTopicName",
                    PROP_PREFIX + "routingKey.source");
        }

        try {
            connection = factory.newConnection();
            channel = connection.createChannel();
            channel.confirmSelect();

            if (!isTopicRoutingKeySource() && config.isAutoCreateRoutingKey()) {
                final var routingKeyName = config.getRoutingKey();
                LOGGER.info("Creating queue for routing key named '{}'", routingKeyName);
                channel.queueDeclare(routingKeyName, config.isRoutingKeyDurable(), false, false, null);
            }
        }
        catch (IOException | TimeoutException e) {
            throw new DebeziumException(e);
        }
    }

    @PreDestroy
    @Override
    public void close() {

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

            final String exchangeName = (config.getExchange() != null && !config.getExchange().isEmpty())
                    ? config.getExchange()
                    : streamNameMapper.map(record.destination());
            final String routingKeyName = getRoutingKey(record);

            try {
                if (isTopicRoutingKeySource() && config.isAutoCreateRoutingKey()) {
                    LOGGER.trace("Creating queue for routing key named '{}'", routingKeyName);
                    channel.queueDeclare(routingKeyName, config.isRoutingKeyDurable(), false, false, null);
                }

                final Object value = (record.value() != null) ? record.value() : config.getNullValue();
                channel.basicPublish(exchangeName, routingKeyName,
                        new AMQP.BasicProperties.Builder()
                                .deliveryMode(config.getDeliveryMode())
                                .headers(convertRabbitMqHeaders(record))
                                .build(),
                        getBytes(value));
            }
            catch (IOException e) {
                throw new DebeziumException(e);
            }
        }

        try {
            channel.waitForConfirmsOrDie(config.getAckTimeout());
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

    private String getRoutingKey(ChangeEvent<Object, Object> eventRecord) {
        if (isStaticRoutingKeySource()) {
            return config.getRoutingKey();
        }
        else if (isTopicRoutingKeySource()) {
            return streamNameMapper.map(eventRecord.destination());
        }
        else if (isKeyRoutingKeySource()) {
            return eventRecord.key() != null ? getString(eventRecord.key()) : EMPTY_ROUTING_KEY;
        }
        return EMPTY_ROUTING_KEY;
    }

    private boolean isStaticRoutingKeySource() {
        return STATIC_ROUTING_KEY_SOURCE.equals(config.getRoutingKeySource());
    }

    private boolean isTopicRoutingKeySource() {
        return TOPIC_ROUTING_KEY_SOURCE.equals(config.getRoutingKeySource());
    }

    private boolean isKeyRoutingKeySource() {
        return KEY_ROUTING_KEY_SOURCE.equals(config.getRoutingKeySource());
    }

    private static Map<String, Object> convertRabbitMqHeaders(ChangeEvent<Object, Object> record) {
        List<Header<Object>> headers = record.headers();
        Map<String, Object> rabbitMqHeaders = new HashMap<>();
        for (Header<Object> header : headers) {
            rabbitMqHeaders.put(header.getKey(), header.getValue());
        }
        return rabbitMqHeaders;
    }

    @VisibleForTesting
    void setStreamNameMapper(StreamNameMapper streamNameMapper) {
        this.streamNameMapper = streamNameMapper;
    }

    @Override
    public Field.Set getConfigFields() {
        return Field.setOf(
                RabbitMqStreamChangeConsumerConfig.EXCHANGE,
                RabbitMqStreamChangeConsumerConfig.ROUTING_KEY,
                RabbitMqStreamChangeConsumerConfig.AUTO_CREATE_ROUTING_KEY,
                RabbitMqStreamChangeConsumerConfig.ROUTING_KEY_DURABLE,
                RabbitMqStreamChangeConsumerConfig.ROUTING_KEY_SOURCE,
                RabbitMqStreamChangeConsumerConfig.ROUTING_KEY_FROM_TOPIC_NAME,
                RabbitMqStreamChangeConsumerConfig.DELIVERY_MODE,
                RabbitMqStreamChangeConsumerConfig.ACK_TIMEOUT,
                RabbitMqStreamChangeConsumerConfig.NULL_VALUE);
    }

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(componentMetadataFactory.createComponentMetadata(this, Module.version()));
    }
}
