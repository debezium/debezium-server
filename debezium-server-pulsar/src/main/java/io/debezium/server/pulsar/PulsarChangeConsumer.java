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

import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import io.debezium.server.api.DebeziumServerConsumer;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.Module;
import io.debezium.config.Field;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataFactory;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.api.DebeziumServerSink;

/**
 * Implementation of the consumer that delivers the messages into a Pulsar destination.
 *
 * @author Jiri Pechanec
 * @author Henrik Schnell
 *
 */
@Named("pulsar")
@Dependent
public class PulsarChangeConsumer extends BaseChangeConsumer implements DebeziumServerConsumer<CapturingEvents<BatchEvent>>, DebeziumServerSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarChangeConsumer.class);

    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();

    private static final String PROP_PREFIX = "debezium.sink.pulsar.";
    private static final String PROP_CLIENT_PREFIX = PROP_PREFIX + "client.";
    private static final String PROP_PRODUCER_PREFIX = PROP_PREFIX + "producer.";

    private static final AtomicBoolean hasFailed = new AtomicBoolean(false);

    public interface ProducerBuilder {
        Producer<Object> get(String topicName, Object value);
    }

    private PulsarChangeConsumerConfig config;
    private final Map<String, Producer<?>> producers = new HashMap<>();
    private PulsarClient pulsarClient;
    private Map<String, Object> producerConfig;

    @PostConstruct
    void connect() {
        final Config mpConfig = ConfigProvider.getConfig();

        // Load configuration
        io.debezium.config.Configuration configuration = io.debezium.config.Configuration.from(getConfigSubset(mpConfig, PROP_PREFIX));
        this.config = new PulsarChangeConsumerConfig(configuration);

        try {
            pulsarClient = PulsarClient.builder()
                    .loadConf(getConfigSubset(mpConfig, PROP_CLIENT_PREFIX))
                    .build();
        }
        catch (PulsarClientException e) {
            throw new DebeziumException(e);
        }
        producerConfig = getConfigSubset(mpConfig, PROP_PRODUCER_PREFIX);
    }

    @PreDestroy
    @Override
    public void close() {
        final List<CompletableFuture<Void>> closeFutures = new ArrayList<>(producers.size());
        producers.values().forEach(producer -> {
            // Avoid potentially infinitely long blocking call if things go wrong.
            closeFutures.add(producer.closeAsync().orTimeout(config.getTimeout(), TimeUnit.MILLISECONDS));
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
        final String topicFullName = config.getPulsarTenant() + "/" + config.getPulsarNamespace() + "/" + topicName;
        try {
            if (value instanceof String) {
                return pulsarClient.newProducer(Schema.STRING)
                        .loadConf(producerConfig)
                        .topic(topicFullName)
                        .batcherBuilder(getBatcherBuilder(config.getBatcherBuilder()))
                        .create();
            }
            else {
                return pulsarClient.newProducer()
                        .loadConf(producerConfig)
                        .topic(topicFullName)
                        .batcherBuilder(getBatcherBuilder(config.getBatcherBuilder()))
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
    public void handle(CapturingEvents<BatchEvent> events)
            throws InterruptedException {
        final Map<String, Producer<?>> batchProducers = new HashMap<>();

        for (final BatchEvent record : events.records()) {
            LOGGER.trace("Received event '{}'", record);
            final String topicName = streamNameMapper.map(events.destination());
            final Producer<?> producer = producers.computeIfAbsent(topicName, (topic) -> createProducer(topic, record.value()));
            batchProducers.put(topicName, producer);

            final String key = (record.key()) == null ? config.getNullKey() : getString(record.key());
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
                            record.commit();
                        }
                        else {
                            LOGGER.error("Failed to send record to "+ events.destination() + " destination", exception);
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
            if (config.getTimeout() > 0) {
                allProducersCompleted.get(config.getTimeout(), TimeUnit.MILLISECONDS);
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
    }

    @Override
    public Field.Set getConfigFields() {
        return Field.setOf(
                PulsarChangeConsumerConfig.NULL_KEY,
                PulsarChangeConsumerConfig.TENANT,
                PulsarChangeConsumerConfig.NAMESPACE,
                PulsarChangeConsumerConfig.TIMEOUT,
                PulsarChangeConsumerConfig.PRODUCER_BATCHER_BUILDER);
    }

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(componentMetadataFactory.createComponentMetadata(this, Module.version()));
    }
}
