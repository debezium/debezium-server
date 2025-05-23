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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;

import org.apache.pulsar.client.api.BatcherBuilder;
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
 * @author Henrik Schnell
 *
 */
@Named("pulsar")
@Dependent
public class PulsarChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.pulsar.";
    private static final String PROP_CLIENT_PREFIX = PROP_PREFIX + "client.";
    private static final String PROP_PRODUCER_PREFIX = PROP_PREFIX + "producer.";

    private static final AtomicBoolean hasFailed = new AtomicBoolean(false);

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

    @ConfigProperty(name = PROP_PREFIX + "timeout", defaultValue = "0")
    Integer timeout;

    @ConfigProperty(name = PROP_PRODUCER_PREFIX + "batcherBuilder", defaultValue = "DEFAULT")
    String batcherBuilderConfig;

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
        final List<CompletableFuture<Void>> closeFutures = new ArrayList<>(producers.size());
        producers.values().forEach(producer -> {
            // Avoid potentially infinitely long blocking call if things go wrong.
            closeFutures.add(producer.closeAsync().orTimeout(timeout, TimeUnit.MILLISECONDS));
        });
        for (CompletableFuture<Void> cf : closeFutures) {
            try {
                cf.get();
            }
            catch (Exception e) {
                hasFailed.set(true);
                LOGGER.warn("Exception while closing producer", e);
            }
        }
        try {
            // DBZ-8843 If the client failed, terminate it abruptly to prevent client restart during or after server shutdown.
            if (hasFailed.get()) {
                LOGGER.warn("Shutting down pulsar client forcefully due to previous failure");
                pulsarClient.shutdown();
            }
            else {
                pulsarClient.close();
            }
        }
        catch (Exception e) {
            LOGGER.warn("Exception while closing client", e);
        }
    }

    private Producer<?> createProducer(String topicName, Object value) {
        final String topicFullName = pulsarTenant + "/" + pulsarNamespace + "/" + topicName;
        try {
            if (value instanceof String) {
                return pulsarClient.newProducer(Schema.STRING)
                        .loadConf(producerConfig)
                        .topic(topicFullName)
                        .batcherBuilder(getBatcherBuilder(batcherBuilderConfig))
                        .create();
            }
            else {
                return pulsarClient.newProducer()
                        .loadConf(producerConfig)
                        .topic(topicFullName)
                        .batcherBuilder(getBatcherBuilder(batcherBuilderConfig))
                        .create();
            }
        }
        catch (PulsarClientException e) {
            hasFailed.set(true);
            throw new DebeziumException(e);
        }
    }

    private BatcherBuilder getBatcherBuilder(String configValue) {
        switch (configValue) {
            case "KEY_BASED":
                return BatcherBuilder.KEY_BASED;
            case "DEFAULT":
            default:
                return BatcherBuilder.DEFAULT;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        final Map<String, Producer<?>> batchProducers = new HashMap<>();

        for (final ChangeEvent<Object, Object> record : records) {
            LOGGER.trace("Received event '{}'", record);
            final String topicName = streamNameMapper.map(record.destination());
            final Producer<?> producer = producers.computeIfAbsent(topicName, (topic) -> createProducer(topic, record.value()));
            batchProducers.put(topicName, producer);

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

            // We will wait for the producers to flush instead of waiting for these individually
            message.sendAsync()
                    .whenComplete((messageId, exception) -> {
                        if (exception == null) {
                            LOGGER.trace("Sent message with id: {}", messageId);
                            try {
                                committer.markProcessed(record);
                            }
                            catch (InterruptedException e) {
                                throw new DebeziumException(e);
                            }
                        }
                        else {
                            LOGGER.error("Failed to send record to {} destination", record.destination(), exception);
                        }
                    });
        }

        // Flush all producers asynchronously
        // Waiting for the returned futures will wait until all messages have been successfully persisted.
        CompletableFuture<Void> allProducersCompleted = CompletableFuture
                .allOf(batchProducers
                        .values()
                        .stream()
                        .map(Producer::flushAsync)
                        .toArray(CompletableFuture[]::new));

        try {
            // Wait for all producers to complete the flush with timeout
            if (timeout > 0) {
                allProducersCompleted.get(timeout, TimeUnit.MILLISECONDS);
            }
            else {
                allProducersCompleted.join();
            }
        }
        catch (CompletionException | ExecutionException | TimeoutException exception) {
            hasFailed.set(true);
            LOGGER.error("Failed to send batch", exception);
            throw new DebeziumException(exception);
        }

        committer.markBatchFinished();
    }
}
