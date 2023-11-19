/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConnectionFactoryConfigurator;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.StreamException;
import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.engine.Header;
import io.debezium.server.BaseChangeConsumer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Implementation of the consumer that delivers the messages into RabbitMQ Stream destination.
 *
 * @author Olivier Boudet
 *
 */
@Named("rabbitmqstream")
@Dependent
public class RabbitMqStreamNativeChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqStreamNativeChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.rabbitmqstream.";
    private static final String PROP_CONNECTION_PREFIX = PROP_PREFIX + "connection.";
    private static final String STREAM_NAME = PROP_PREFIX + "stream";
    private String stream;

    /**
     * When true, the routing key is calculated from topic name using stream name mapper.
     * When false the routingKey value or empty string is used.
     */

    @ConfigProperty(name = PROP_PREFIX + "deliveryMode", defaultValue = "2")
    int deliveryMode;

    @ConfigProperty(name = PROP_PREFIX + "ackTimeout", defaultValue = "30000")
    int ackTimeout;

    @ConfigProperty(name = PROP_PREFIX + "null.value", defaultValue = "default")
    String nullValue;

    Environment environment;

    Producer producer;

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
            environment = Environment.builder()
                    .host(factory.getHost())
                    .port(factory.getPort()).build();

            stream = config.getValue(STREAM_NAME, String.class);

            LOGGER.info("Creating stream '{}'", stream);

            environment.streamCreator().stream(stream).create();

            producer = environment.producerBuilder()
                    .confirmTimeout(Duration.ofSeconds(ackTimeout))
                    .stream(stream)
                    .build();

        }
        catch (StreamException | IllegalArgumentException e) {
            throw new DebeziumException(e);
        }
    }

    @PreDestroy
    void close() {

        try {
            if (producer != null) {
                producer.close();
            }
            if (environment != null) {
                environment.close();
            }
        }
        catch (Exception e) {
            throw new DebeziumException(e);
        }

    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        for (ChangeEvent<Object, Object> record : records) {
            LOGGER.trace("Received event '{}'", record);
            try {
                final Object value = (record.value() != null) ? record.value() : nullValue;
                producer.send(
                        producer.messageBuilder().addData(getBytes(value)).build(),
                        confirmationStatus -> {
                        });

            }
            catch (StreamException e) {
                throw new DebeziumException(e);
            }

            committer.markProcessed(record);
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
