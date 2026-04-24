/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import io.debezium.server.api.DebeziumServerConsumer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
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
import io.debezium.engine.Header;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataFactory;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.CustomConsumerBuilder;
import io.debezium.server.api.DebeziumServerSink;

/**
 * An implementation of the {@link DebeziumEngine.ChangeConsumer} interface that publishes change event messages to Kafka.
 */
@Named("kafka")
@Dependent
public class KafkaChangeConsumer extends BaseChangeConsumer implements DebeziumServerConsumer<CapturingEvents<BatchEvent>>, DebeziumServerSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaChangeConsumer.class);

    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();

    private static final String PROP_PREFIX = "debezium.sink.kafka.";

    private static final String PROP_PREFIX_PRODUCER = PROP_PREFIX + "producer.";

    private KafkaChangeConsumerConfig config;

    private KafkaProducer<Object, Object> producer;

    @Inject
    @CustomConsumerBuilder
    Instance<KafkaProducer<Object, Object>> customKafkaProducer;

    @PostConstruct
    void start() {
        if (customKafkaProducer.isResolvable()) {
            producer = customKafkaProducer.get();
            LOGGER.info("Obtained custom configured KafkaProducer '{}'", producer);
            return;
        }

        final Config mpConfig = ConfigProvider.getConfig();

        // Load configuration
        io.debezium.config.Configuration configuration = io.debezium.config.Configuration.from(getConfigSubset(mpConfig, PROP_PREFIX));
        this.config = new KafkaChangeConsumerConfig(configuration);

        producer = new KafkaProducer<>(getConfigSubset(mpConfig, PROP_PREFIX_PRODUCER));
        LOGGER.info("consumer started...");
    }

    @PreDestroy
    @Override
    public void close() {
        LOGGER.info("consumer destroyed...");
        if (producer != null) {
            try {
                producer.close(Duration.ofSeconds(5));
            }
            catch (Throwable t) {
                LOGGER.warn("Could not close producer", t);
            }
        }
    }

    @Override
    public void handle(final CapturingEvents<BatchEvent> events) throws InterruptedException {

        final List<Future<RecordMetadata>> deliveryFutures = new ArrayList<>(events.records().size());

        for (BatchEvent record : events.records()) {
            try {
                LOGGER.trace("Received event '{}'", record);
                Headers headers = convertKafkaHeaders(record);

                String topicName = streamNameMapper.map(events.destination());
                deliveryFutures.add(producer.send(new ProducerRecord<>(topicName, null, null, record.key(), record.value(), headers),
                        (metadata, exception) -> {
                            if (exception != null) {
                                LOGGER.error("Failed to send record with key '{}' to {}:", asString(record.key()), topicName,
                                        exception);
                                throw new DebeziumException(exception);
                            }
                            else {
                                LOGGER.trace("Sent message with offset: {}", metadata.offset());
                            }
                        }));
            }
            catch (Exception e) {
                throw new DebeziumException(e);
            }
        }

        try {
            for (int i = 0; i < events.records().size(); i++) {
                final var recordMetadataFuture = deliveryFutures.get(i);
                final var record = events.records().get(i);

                if (config.getWaitMessageDeliveryTimeout() == 0) {
                    recordMetadataFuture.get();
                }
                else {
                    try {
                        recordMetadataFuture.get(config.getWaitMessageDeliveryTimeout(), TimeUnit.MILLISECONDS);
                    }
                    catch (TimeoutException e) {
                        LOGGER.error("Timed out while waiting to send a record to '{}'", streamNameMapper.map(events.destination()));
                        throw new DebeziumException(e);
                    }

                }
                record.commit();
            }
        }
        catch (ExecutionException e) {
            throw new DebeziumException(e);
        }
    }

    private Headers convertKafkaHeaders(BatchEvent record) {
        List<Header<Object>> headers = record.headers();
        Headers kafkaHeaders = new RecordHeaders();
        for (Header<Object> header : headers) {
            kafkaHeaders.add(header.getKey(), getBytes(header.getValue()));
        }
        return kafkaHeaders;
    }

    @Override
    public Field.Set getConfigFields() {
        return Field.setOf(KafkaChangeConsumerConfig.WAIT_MESSAGE_DELIVERY_TIMEOUT_MS);
    }

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(componentMetadataFactory.createComponentMetadata(this, Module.version()));
    }
}
