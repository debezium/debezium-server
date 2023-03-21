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

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.inject.Named;

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
public class RabbitmqStreamChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitmqStreamChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.rabbitmq.";

    @ConfigProperty(name = PROP_PREFIX + "routingKey", defaultValue = "")
    Optional<String> routingKey;

    @ConfigProperty(name = PROP_PREFIX + "ackTimeout", defaultValue = "30000")
    int ackTimeout;

    Connection connection;

    Channel channel;

    @PostConstruct
    void connect() {
        final Config config = ConfigProvider.getConfig();

        ConnectionFactory factory = new ConnectionFactory();
        Map<String, String> configProperties = getConfigSubset(config, PROP_PREFIX).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> (String) entry.getValue()));
        ConnectionFactoryConfigurator.load(factory, configProperties, "");

        LOGGER.info("Using connection to {}:{}", factory.getHost(), factory.getPort());

        try {
            connection = factory.newConnection();
            channel = connection.createChannel();
            channel.confirmSelect();
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
            final String exchange = streamNameMapper.map(record.destination());

            try {
                channel.basicPublish(exchange, routingKey.orElse(record.destination()),
                        new AMQP.BasicProperties.Builder()
                                .headers(convertRabbitMqHeaders(record))
                                .build(),
                        getBytes(record.value()));
            }
            catch (IOException e) {
                throw new DebeziumException(e);
            }

            committer.markProcessed(record);
        }

        try {
            channel.waitForConfirmsOrDie(ackTimeout);
        }
        catch (IOException | TimeoutException e) {
            throw new DebeziumException(e);
        }

        LOGGER.trace("Sent messages");
        committer.markBatchFinished();
    }

    @Override
    public boolean supportsTombstoneEvents() {
        return false;
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
