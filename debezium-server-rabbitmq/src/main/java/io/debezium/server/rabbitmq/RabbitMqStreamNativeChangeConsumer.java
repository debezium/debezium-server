/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.rabbitmq;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConnectionFactoryConfigurator;
import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.ByteCapacity;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.StreamCreator;
import com.rabbitmq.stream.StreamException;

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
@Named("rabbitmqstream")
@Dependent
public class RabbitMqStreamNativeChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqStreamNativeChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.rabbitmqstream.";

    private static final String PROP_STREAM = PROP_PREFIX + "stream";
    private static final String PROP_CONNECTION_PREFIX = PROP_PREFIX + "connection.";

    @ConfigProperty(name = PROP_STREAM)
    Optional<String> stream;

    @ConfigProperty(name = PROP_PREFIX + "stream.maxAge")
    Optional<Duration> streamMaxAge;

    @ConfigProperty(name = PROP_PREFIX + "stream.maxLength")
    Optional<String> streamMaxLength;

    @ConfigProperty(name = PROP_PREFIX + "stream.maxSegmentSize")
    Optional<String> streamMaxSegmentSize;

    @ConfigProperty(name = PROP_PREFIX + "ackTimeout", defaultValue = "30000")
    int ackTimeout;

    @ConfigProperty(name = PROP_PREFIX + "null.value", defaultValue = "default")
    String nullValue;

    Environment environment;

    Map<String, Producer> streamProducers = new HashMap<>();

    private void createStream(Environment env, String name) {
        LOGGER.info("Creating stream '{}'", name);
        StreamCreator stream = env.streamCreator().stream(name);

        streamMaxAge.ifPresent(stream::maxAge);
        streamMaxLength.map(ByteCapacity::from).ifPresent(stream::maxLengthBytes);
        streamMaxSegmentSize.map(ByteCapacity::from).ifPresent(stream::maxSegmentSizeBytes);

        stream.create();
    }

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
            Address entryPoint = new Address(factory.getHost(), factory.getPort());
            environment = Environment.builder()
                    .host(entryPoint.host())
                    .port(entryPoint.port())
                    .addressResolver(address -> entryPoint)
                    .username(factory.getUsername())
                    .password(factory.getPassword())
                    .virtualHost(factory.getVirtualHost())
                    .requestedMaxFrameSize(factory.getRequestedFrameMax())
                    .build();
        }
        catch (StreamException | IllegalArgumentException e) {
            throw new DebeziumException(e);
        }
    }

    @PreDestroy
    void close() {

        try {
            if (environment != null) {
                environment.close();
            }
            if (streamProducers != null) {
                for (Producer producer : streamProducers.values()) {
                    producer.close();
                }
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
                String topic = stream.orElse(streamNameMapper.map(record.destination()));

                Producer producer = streamProducers.get(topic);
                if (producer == null) {
                    if (!environment.streamExists(topic)) {
                        createStream(environment, topic);
                    }

                    producer = environment.producerBuilder()
                            .confirmTimeout(Duration.ofSeconds(ackTimeout))
                            .stream(topic)
                            .build();

                    streamProducers.put(topic, producer);
                }

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

    private Map<String, Object> convertRabbitMqHeaders(ChangeEvent<Object, Object> record) {
        List<Header<Object>> headers = record.headers();
        Map<String, Object> rabbitMqHeaders = new HashMap<>();
        for (Header<Object> header : headers) {
            rabbitMqHeaders.put(header.getKey(), header.getValue());
        }
        return rabbitMqHeaders;
    }

}
