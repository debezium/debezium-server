/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.rabbitmq;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.ByteCapacity;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.EnvironmentBuilder;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageBuilder;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.ProducerBuilder;
import com.rabbitmq.stream.StreamCreator;
import com.rabbitmq.stream.StreamException;

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
import io.debezium.server.DebeziumServerSink;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;

/**
 * Implementation of the consumer that delivers the messages into RabbitMQ Stream destination.
 *
 * @author Olivier Boudet
 */
@Named("rabbitmqstream")
@Dependent
public class RabbitMqStreamNativeChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>>, DebeziumServerSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqStreamNativeChangeConsumer.class);

    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();

    private static final String PROP_PREFIX = "debezium.sink.rabbitmqstream.";

    private RabbitMqStreamNativeChangeConsumerConfig config;

    Environment environment;

    Map<String, Producer> streamProducers = new HashMap<>();

    private void createStream(Environment env, String name) {
        StreamCreator stream = env.streamCreator();
        if (config.isSuperStreamEnable()) {
            LOGGER.info("Creating super stream '{}' with {} partitions", name, config.getSuperStreamPartitions());
            StreamCreator.SuperStreamConfiguration superStreamConfiguration = stream
                    .name(name)
                    .superStream()
                    .partitions(config.getSuperStreamPartitions());

            if (config.getSuperStreamBindingKeys() != null) {
                superStreamConfiguration.bindingKeys(config.getSuperStreamBindingKeys());
            }
            stream = superStreamConfiguration.creator();
        }
        else {
            LOGGER.info("Creating stream '{}'", name);
            stream = stream.stream(name);
        }

        if (config.getStreamMaxAge() != null) {
            stream.maxAge(config.getStreamMaxAge());
        }
        if (config.getStreamMaxLength() != null) {
            stream.maxLengthBytes(ByteCapacity.from(config.getStreamMaxLength()));
        }
        if (config.getStreamMaxSegmentSize() != null) {
            stream.maxSegmentSizeBytes(ByteCapacity.from(config.getStreamMaxSegmentSize()));
        }

        stream.create();
    }

    private Message createMessage(Producer producer, ChangeEvent<Object, Object> record) {
        MessageBuilder message = producer.messageBuilder();
        MessageBuilder.ApplicationPropertiesBuilder applicationProperties = message.applicationProperties();
        for (Header<Object> header : record.headers()) {
            applicationProperties = applicationProperties.entry(header.getKey(), getString(header.getValue()));
        }
        message = applicationProperties.messageBuilder();
        message = message.properties().messageId(getString(record.key())).messageBuilder();

        final Object value = (record.value() != null) ? record.value() : config.getNullValue();

        return message.addData(getBytes(value)).build();
    }

    private Producer createProducer(String topic) {
        ProducerBuilder producer = environment.producerBuilder();

        if (config.isSuperStreamEnable()) {
            producer = producer
                    .superStream(topic)
                    .routing(msg -> msg.getProperties().getMessageIdAsString())
                    .producerBuilder();
        }
        else {
            producer = producer
                    .stream(topic)
                    .subEntrySize(config.getProducerSubEntrySize());
        }

        if (config.getProducerFilterValue() != null) {
            String filterKey = config.getProducerFilterValue();
            producer = producer.filterValue(msg -> {
                Map<String, Object> properties = msg.getApplicationProperties();
                if (properties == null || !properties.containsKey(filterKey)) {
                    LOGGER.warn("Property '{}' not found in message application properties. Filter will return null.", filterKey);
                    return null;
                }

                Object filterValue = properties.get(filterKey);
                return filterValue != null ? filterValue.toString() : null;
            });
        }

        return producer
                .confirmTimeout(Duration.ofSeconds(config.getProducerConfirmTimeout()))
                .enqueueTimeout(Duration.ofSeconds(config.getProducerEnqueueTimeout()))
                .batchPublishingDelay(Duration.ofMillis(config.getProducerBatchPublishingDelay()))
                .maxUnconfirmedMessages(config.getProducerMaxUnconfirmedMessages())
                .batchSize(config.getProducerBatchSize())
                .name(config.getProducerName())
                .build();
    }

    @PostConstruct
    void connect() {
        final Config mpConfig = ConfigProvider.getConfig();

        // Load configuration
        io.debezium.config.Configuration configuration = io.debezium.config.Configuration.from(getConfigSubset(mpConfig, PROP_PREFIX));
        this.config = new RabbitMqStreamNativeChangeConsumerConfig(configuration);

        if (config.getConnectionHost() != null || config.getConnectionPort() != null) {
            LOGGER.warn("The parameters connection.host and connection.port are deprecated, please use rabbitmqstream.host and rabbitmqstream.port moving forward.");
        }

        final String connectionHost = (config.getConnectionHost() != null) ? config.getConnectionHost() : config.getHost();
        final int connectionPort = (config.getConnectionPort() != null) ? config.getConnectionPort() : config.getPort();

        LOGGER.info("Using connection to {}:{}", connectionHost, connectionPort);

        try {
            Address entryPoint = new Address(connectionHost, connectionPort);
            EnvironmentBuilder environmentBuilder = Environment.builder();

            if (config.isTlsEnable()) {
                try {
                    SslContext sslContext = SslContextBuilder.forClient()
                            .sslProvider(SslProvider.JDK)
                            .protocols("TLSv1.2", "TLSv1.3")
                            .build();

                    if (config.getTlsServerName() != null) {
                        SSLParameters sslParameters = new SSLParameters();
                        sslParameters.setServerNames(Collections.singletonList(new SNIHostName(config.getTlsServerName())));

                        SSLEngine sslEngine = sslContext.newEngine(ByteBufAllocator.DEFAULT);
                        sslEngine.setSSLParameters(sslParameters);
                    }

                    environmentBuilder = environmentBuilder
                            .tls()
                            .sslContext(sslContext)
                            .environmentBuilder();
                }
                catch (SSLException e) {
                    LOGGER.error("Failed to set SSL context: {}", e.getMessage());
                }
            }

            environment = environmentBuilder
                    .host(entryPoint.host())
                    .port(entryPoint.port())
                    .addressResolver(address -> entryPoint)
                    .username(config.getUsername())
                    .password(config.getPassword())
                    .virtualHost(config.getVirtualHost())
                    .requestedMaxFrameSize(config.getRequestedMaxFrameSize())
                    .requestedHeartbeat(Duration.ofSeconds(config.getRequestedHeartbeat()))
                    .rpcTimeout(Duration.ofSeconds(config.getRpcTimeout()))
                    .maxProducersByConnection(config.getMaxProducersByConnection())
                    .maxTrackingConsumersByConnection(config.getMaxTrackingConsumersByConnection())
                    .maxConsumersByConnection(config.getMaxConsumersByConnection())
                    .id(config.getId())
                    .build();
        }
        catch (StreamException | IllegalArgumentException e) {
            throw new DebeziumException(e);
        }
    }

    @PreDestroy
    @Override
    public void close() {

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

        CountDownLatch latch = new CountDownLatch(records.size());
        AtomicBoolean hasError = new AtomicBoolean(false);

        for (ChangeEvent<Object, Object> record : records) {
            LOGGER.trace("Received event '{}'", record);

            try {
                String topic = (config.getStream() != null) ? config.getStream() : streamNameMapper.map(record.destination());

                Producer producer = streamProducers.get(topic);
                if (producer == null) {
                    if (!environment.streamExists(topic)) {
                        createStream(environment, topic);
                    }

                    producer = createProducer(topic);
                    streamProducers.put(topic, producer);
                }

                producer.send(
                        createMessage(producer, record),
                        confirmationStatus -> {
                            try {
                                if (confirmationStatus.isConfirmed()) {
                                    committer.markProcessed(record);
                                }
                                else {
                                    LOGGER.error("Failed to confirm message delivery for event '{}'", record);
                                    hasError.set(true);
                                }
                            }
                            catch (Exception e) {
                                LOGGER.error("Failed to process record '{}': {}", record, e.getMessage());
                                hasError.set(true);
                            }
                            finally {
                                latch.countDown();
                            }
                        });
            }
            catch (StreamException e) {
                throw new DebeziumException(e);
            }
        }

        if (!latch.await(config.getBatchConfirmTimeout(), TimeUnit.SECONDS)) {
            LOGGER.warn("Timeout while waiting for batch confirmation");
            hasError.set(true);
        }

        if (!hasError.get()) {
            LOGGER.trace("All messages sent successfully");
            committer.markBatchFinished();
        }
        else {
            throw new DebeziumException("Batch processing was incomplete due to record processing errors.");
        }
    }

    @Override
    public Field.Set getConfigFields() {
        return Field.setOf(
                RabbitMqStreamNativeChangeConsumerConfig.CONNECTION_HOST,
                RabbitMqStreamNativeChangeConsumerConfig.CONNECTION_PORT,
                RabbitMqStreamNativeChangeConsumerConfig.HOST,
                RabbitMqStreamNativeChangeConsumerConfig.PORT,
                RabbitMqStreamNativeChangeConsumerConfig.USERNAME,
                RabbitMqStreamNativeChangeConsumerConfig.PASSWORD,
                RabbitMqStreamNativeChangeConsumerConfig.VIRTUAL_HOST,
                RabbitMqStreamNativeChangeConsumerConfig.TLS_ENABLE,
                RabbitMqStreamNativeChangeConsumerConfig.TLS_SERVER_NAME,
                RabbitMqStreamNativeChangeConsumerConfig.RPC_TIMEOUT,
                RabbitMqStreamNativeChangeConsumerConfig.MAX_PRODUCERS_BY_CONNECTION,
                RabbitMqStreamNativeChangeConsumerConfig.MAX_TRACKING_CONSUMERS_BY_CONNECTION,
                RabbitMqStreamNativeChangeConsumerConfig.MAX_CONSUMERS_BY_CONNECTION,
                RabbitMqStreamNativeChangeConsumerConfig.REQUESTED_HEARTBEAT,
                RabbitMqStreamNativeChangeConsumerConfig.REQUESTED_MAX_FRAME_SIZE,
                RabbitMqStreamNativeChangeConsumerConfig.ID,
                RabbitMqStreamNativeChangeConsumerConfig.STREAM,
                RabbitMqStreamNativeChangeConsumerConfig.STREAM_MAX_AGE,
                RabbitMqStreamNativeChangeConsumerConfig.STREAM_MAX_LENGTH,
                RabbitMqStreamNativeChangeConsumerConfig.STREAM_MAX_SEGMENT_SIZE,
                RabbitMqStreamNativeChangeConsumerConfig.SUPER_STREAM_ENABLE,
                RabbitMqStreamNativeChangeConsumerConfig.SUPER_STREAM_PARTITIONS,
                RabbitMqStreamNativeChangeConsumerConfig.SUPER_STREAM_BINDING_KEYS,
                RabbitMqStreamNativeChangeConsumerConfig.PRODUCER_NAME,
                RabbitMqStreamNativeChangeConsumerConfig.PRODUCER_FILTER_VALUE,
                RabbitMqStreamNativeChangeConsumerConfig.PRODUCER_BATCH_SIZE,
                RabbitMqStreamNativeChangeConsumerConfig.PRODUCER_SUB_ENTRY_SIZE,
                RabbitMqStreamNativeChangeConsumerConfig.PRODUCER_MAX_UNCONFIRMED_MESSAGES,
                RabbitMqStreamNativeChangeConsumerConfig.PRODUCER_BATCH_PUBLISHING_DELAY,
                RabbitMqStreamNativeChangeConsumerConfig.PRODUCER_CONFIRM_TIMEOUT,
                RabbitMqStreamNativeChangeConsumerConfig.PRODUCER_ENQUEUE_TIMEOUT,
                RabbitMqStreamNativeChangeConsumerConfig.BATCH_CONFIRM_TIMEOUT,
                RabbitMqStreamNativeChangeConsumerConfig.NULL_VALUE);
    }

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(componentMetadataFactory.createComponentMetadata(this, Module.version()));
    }
}
