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
import java.util.Optional;
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

import org.eclipse.microprofile.config.inject.ConfigProperty;
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
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.engine.Header;
import io.debezium.server.BaseChangeConsumer;
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
public class RabbitMqStreamNativeChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqStreamNativeChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.rabbitmqstream.";

    @Deprecated
    @ConfigProperty(name = PROP_PREFIX + "connection.host")
    Optional<String> legacyHost;

    @Deprecated
    @ConfigProperty(name = PROP_PREFIX + "connection.port")
    Optional<Integer> legacyPort;

    @ConfigProperty(name = PROP_PREFIX + "host", defaultValue = "localhost")
    String host;

    @ConfigProperty(name = PROP_PREFIX + "port", defaultValue = "5552")
    int port;

    @ConfigProperty(name = PROP_PREFIX + "username", defaultValue = "guest")
    String username;

    @ConfigProperty(name = PROP_PREFIX + "password", defaultValue = "guest")
    String password;

    @ConfigProperty(name = PROP_PREFIX + "virtualHost", defaultValue = "/")
    String virtualHost;

    @ConfigProperty(name = PROP_PREFIX + "tls.enable", defaultValue = "false")
    boolean tlsEnable;

    @ConfigProperty(name = PROP_PREFIX + "tls.serverName")
    Optional<String> tlsServerName;

    @ConfigProperty(name = PROP_PREFIX + "rpcTimeout", defaultValue = "10")
    int rpcTimeout;

    @ConfigProperty(name = PROP_PREFIX + "maxProducersByConnection", defaultValue = "256")
    int maxProducersByConnection;

    @ConfigProperty(name = PROP_PREFIX + "maxTrackingConsumersByConnection", defaultValue = "50")
    int maxTrackingConsumersByConnection;

    @ConfigProperty(name = PROP_PREFIX + "maxConsumersByConnection", defaultValue = "256")
    int maxConsumersByConnection;

    @ConfigProperty(name = PROP_PREFIX + "requestedHeartbeat", defaultValue = "60")
    int requestedHeartbeat;

    @ConfigProperty(name = PROP_PREFIX + "requestedMaxFrameSize", defaultValue = "0")
    int requestedMaxFrameSize;

    @ConfigProperty(name = PROP_PREFIX + "id", defaultValue = "rabbitmq-stream")
    String id;

    @ConfigProperty(name = PROP_PREFIX + "stream")
    Optional<String> stream;

    @ConfigProperty(name = PROP_PREFIX + "stream.maxAge")
    Optional<Duration> streamMaxAge;

    @ConfigProperty(name = PROP_PREFIX + "stream.maxLength")
    Optional<String> streamMaxLength;

    @ConfigProperty(name = PROP_PREFIX + "stream.maxSegmentSize")
    Optional<String> streamMaxSegmentSize;

    @ConfigProperty(name = PROP_PREFIX + "superStream.enable", defaultValue = "false")
    boolean superStreamEnable;

    @ConfigProperty(name = PROP_PREFIX + "superStream.partitions", defaultValue = "3")
    int superStreamPartitions;

    @ConfigProperty(name = PROP_PREFIX + "superStream.bindingKeys")
    Optional<String[]> superStreamBindingKeys;

    @ConfigProperty(name = PROP_PREFIX + "producer.name")
    Optional<String> producerName;

    @ConfigProperty(name = PROP_PREFIX + "producer.filterValue")
    Optional<String> producerFilterValue;

    @ConfigProperty(name = PROP_PREFIX + "producer.batchSize", defaultValue = "100")
    int producerBatchSize;

    @ConfigProperty(name = PROP_PREFIX + "producer.subEntrySize", defaultValue = "1")
    int producerSubEntrySize;

    @ConfigProperty(name = PROP_PREFIX + "producer.maxUnconfirmedMessages", defaultValue = "10000")
    int producerMaxUnconfirmedMessages;

    @ConfigProperty(name = PROP_PREFIX + "producer.batchPublishingDelay", defaultValue = "100")
    int producerBatchPublishingDelay;

    @ConfigProperty(name = PROP_PREFIX + "producer.confirmTimeout", defaultValue = "30")
    int producerConfirmTimeout;

    @ConfigProperty(name = PROP_PREFIX + "producer.enqueueTimeout", defaultValue = "10")
    int producerEnqueueTimeout;

    @ConfigProperty(name = PROP_PREFIX + "batchConfirmTimeout", defaultValue = "30")
    int batchConfirmTimeout;

    @ConfigProperty(name = PROP_PREFIX + "null.value", defaultValue = "default")
    String nullValue;

    Environment environment;

    Map<String, Producer> streamProducers = new HashMap<>();

    private void createStream(Environment env, String name) {
        StreamCreator stream = env.streamCreator();
        if (superStreamEnable) {
            LOGGER.info("Creating super stream '{}' with {} partitions", name, superStreamPartitions);
            StreamCreator.SuperStreamConfiguration superStreamConfiguration = stream
                    .name(name)
                    .superStream()
                    .partitions(superStreamPartitions);

            superStreamBindingKeys.ifPresent(superStreamConfiguration::bindingKeys);
            stream = superStreamConfiguration.creator();
        }
        else {
            LOGGER.info("Creating stream '{}'", name);
            stream = stream.stream(name);
        }

        streamMaxAge.ifPresent(stream::maxAge);
        streamMaxLength.map(ByteCapacity::from).ifPresent(stream::maxLengthBytes);
        streamMaxSegmentSize.map(ByteCapacity::from).ifPresent(stream::maxSegmentSizeBytes);

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

        final Object value = (record.value() != null) ? record.value() : nullValue;

        return message.addData(getBytes(value)).build();
    }

    private Producer createProducer(String topic) {
        ProducerBuilder producer = environment.producerBuilder();

        if (superStreamEnable) {
            producer = producer
                    .superStream(topic)
                    .routing(msg -> msg.getProperties().getMessageIdAsString())
                    .producerBuilder();
        }
        else {
            producer = producer
                    .stream(topic)
                    .subEntrySize(producerSubEntrySize);
        }

        if (producerFilterValue.isPresent()) {
            String filterKey = producerFilterValue.get();
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
                .confirmTimeout(Duration.ofSeconds(producerConfirmTimeout))
                .enqueueTimeout(Duration.ofSeconds(producerEnqueueTimeout))
                .batchPublishingDelay(Duration.ofMillis(producerBatchPublishingDelay))
                .maxUnconfirmedMessages(producerMaxUnconfirmedMessages)
                .batchSize(producerBatchSize)
                .name(producerName.orElse(null))
                .build();
    }

    @PostConstruct
    void connect() {
        if (legacyHost.isPresent() || legacyPort.isPresent()) {
            LOGGER.warn("The parameters connection.host and connection.port are deprecated, please use rabbitmqstream.host and rabbitmqstream.port moving forward.");
        }

        final String connectionHost = legacyHost.orElse(host);
        final int connectionPort = legacyPort.orElse(port);

        LOGGER.info("Using connection to {}:{}", connectionHost, connectionPort);

        try {
            Address entryPoint = new Address(connectionHost, connectionPort);
            EnvironmentBuilder environmentBuilder = Environment.builder();

            if (tlsEnable) {
                try {
                    SslContext sslContext = SslContextBuilder.forClient()
                            .sslProvider(SslProvider.JDK)
                            .protocols("TLSv1.2", "TLSv1.3")
                            .build();

                    if (tlsServerName.isPresent()) {
                        SSLParameters sslParameters = new SSLParameters();
                        sslParameters.setServerNames(Collections.singletonList(new SNIHostName(tlsServerName.get())));

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
                    .username(username)
                    .password(password)
                    .virtualHost(virtualHost)
                    .requestedMaxFrameSize(requestedMaxFrameSize)
                    .requestedHeartbeat(Duration.ofSeconds(requestedHeartbeat))
                    .rpcTimeout(Duration.ofSeconds(rpcTimeout))
                    .maxProducersByConnection(maxProducersByConnection)
                    .maxTrackingConsumersByConnection(maxTrackingConsumersByConnection)
                    .maxConsumersByConnection(maxConsumersByConnection)
                    .id(id)
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

        CountDownLatch latch = new CountDownLatch(records.size());
        AtomicBoolean hasError = new AtomicBoolean(false);

        for (ChangeEvent<Object, Object> record : records) {
            LOGGER.trace("Received event '{}'", record);

            try {
                String topic = stream.orElse(streamNameMapper.map(record.destination()));

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

        if (!latch.await(batchConfirmTimeout, TimeUnit.SECONDS)) {
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
}
