/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.pulsar;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.inject.Named;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.server.BaseChangeConsumer;

/**
 * Implementation of the consumer that delivers the messages into a Pulsar destination.
 *
 * @author Jiri Pechanec
 *
 */
@Named("pulsar")
@Dependent
public class PulsarChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.pulsar.";
    private static final String PROP_CLIENT_PREFIX = PROP_PREFIX + "client.";
    private static final String PROP_PRODUCER_PREFIX = PROP_PREFIX + "producer.";

    public interface ProducerBuilder {
        Producer<Object> get(String topicName, Object value);
    }

    private final Map<String, Producer<?>> producers = new HashMap<>();
    private PulsarClient pulsarClient;
    private Map<String, Object> producerConfig;

    @ConfigProperty(name = PROP_PREFIX + "null.key", defaultValue = "default")
    String nullKey;

    @ConfigProperty(name = PROP_PREFIX + "tenant", defaultValue = "public")
    String pulsarTenant;

    @ConfigProperty(name = PROP_PREFIX + "namespace", defaultValue = "default")
    String pulsarNamespace;

    @PostConstruct
    void connect() {
        final Config config = ConfigProvider.getConfig();
        try {
            pulsarClient = PulsarClient.builder()
                    .loadConf(getConfigSubset(config, PROP_CLIENT_PREFIX))
                    .build();
        }
        catch (PulsarClientException e) {
            throw new DebeziumException(e);
        }
        producerConfig = getConfigSubset(config, PROP_PRODUCER_PREFIX);
    }

    @PreDestroy
    void close() {
        producers.values().forEach(producer -> {
            try {
                producer.close();
            }
            catch (Exception e) {
                LOGGER.warn("Exception while closing producer: {}", e);
            }
        });
        try {
            pulsarClient.close();
        }
        catch (Exception e) {
            LOGGER.warn("Exception while closing client: {}", e);
        }
    }

    private Producer<?> createProducer(String topicName, Object value) {
        final String topicFullName = pulsarTenant + "/" + pulsarNamespace + "/" + topicName;
        try {
            if (value instanceof String) {
                return pulsarClient.newProducer(Schema.STRING)
                        .loadConf(producerConfig)
                        .topic(topicFullName)
                        .create();
            }
            else {
                return pulsarClient.newProducer()
                        .loadConf(producerConfig)
                        .topic(topicFullName)
                        .create();
            }
        }
        catch (PulsarClientException e) {
            throw new DebeziumException(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        List<CompletableFuture<MessageId>> futures = new ArrayList<>();

        for (ChangeEvent<Object, Object> record : records) {
            LOGGER.trace("Received event '{}'", record);
            final String topicName = streamNameMapper.map(record.destination());
            final Producer<?> producer = producers.computeIfAbsent(topicName, (topic) -> createProducer(topic, record.value()));

            final String key = (record.key()) == null ? nullKey : getString(record.key());
            @SuppressWarnings("rawtypes")
            final TypedMessageBuilder message;
            if (record.value() instanceof String) {
                message = producer.newMessage(Schema.STRING);
            }
            else {
                message = producer.newMessage();
            }
            message
                    .properties(convertHeaders(record))
                    .key(key)
                    .value(record.value());

            final ChangeEvent<Object, Object> currentRecord = record;

            CompletableFuture<MessageId> future = message
                .sendAsync()
                .thenAccept(messageId -> {
                    LOGGER.trace("Sent message with id: {}", messageId);
                    try {
                        committer.markProcessed(currentRecord);
                    }
                    catch (InterruptedException e) {
                        throw new DebeziumException(e);
                    }
                })
                .exceptionally(ex -> {
                    Throwable exception = (Throwable) ex;
                    LOGGER.error("Failed to send record to {}:", record.destination(), exception);
                    throw new DebeziumException(exception);
                });

            futures.add(future);
        }

        try {
            // Wait for all CompletableFutures to complete
            CompletableFuture
                .allOf(futures.toArray(new CompletableFuture[0]))
                .join();
        }
        catch (CompletionException e) {
            if (e.getCause() instanceof DebeziumException) {
                throw (DebeziumException) e.getCause();
            }
            else {
                throw new DebeziumException(e);
            }
        }

        committer.markBatchFinished();
    }
}
