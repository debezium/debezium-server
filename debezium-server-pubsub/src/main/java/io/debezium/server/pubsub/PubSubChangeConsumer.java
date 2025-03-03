/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.pubsub;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Publisher.Builder;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.CustomConsumerBuilder;
import io.debezium.util.Threads;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * Implementation of the consumer that delivers the messages into Google Pub/Sub destination.
 *
 * @author Jiri Pechanec
 */
@Named("pubsub")
@Dependent
public class PubSubChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PubSubChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.pubsub.";
    private static final String PROP_PROJECT_ID = PROP_PREFIX + "project.id";

    private static final String DEFAULT_CONCURRENCY_THREADS_STRING = "0";
    private static final int DEFAULT_CONCURRENCY_THREADS = Integer.parseInt(DEFAULT_CONCURRENCY_THREADS_STRING);

    private static final String DEFAULT_COMPRESSION_THRESHOLD_BYTES_STRING = "-1";
    private static final int DEFAULT_COMPRESSION_THRESHOLD_BYTES = Integer.parseInt(DEFAULT_COMPRESSION_THRESHOLD_BYTES_STRING);

    public interface PublisherBuilder {
        Publisher get(ProjectTopicName topicName);
    }

    private String projectId;

    private final Map<String, Publisher> publishers = new HashMap<>();
    private PublisherBuilder publisherBuilder;

    @ConfigProperty(name = PROP_PREFIX + "ordering.enabled", defaultValue = "true")
    boolean orderingEnabled;

    @ConfigProperty(name = PROP_PREFIX + "ordering.key")
    Optional<String> orderingKey;

    @ConfigProperty(name = PROP_PREFIX + "null.key", defaultValue = "default")
    String nullKey;

    @ConfigProperty(name = PROP_PREFIX + "batch.delay.threshold.ms", defaultValue = "100")
    Integer maxDelayThresholdMs;

    @ConfigProperty(name = PROP_PREFIX + "batch.element.count.threshold", defaultValue = "100")
    Long maxBufferSize;

    @ConfigProperty(name = PROP_PREFIX + "batch.request.byte.threshold", defaultValue = "10000000")
    Long maxBufferBytes;

    @ConfigProperty(name = PROP_PREFIX + "flowcontrol.enabled", defaultValue = "false")
    boolean flowControlEnabled;

    @ConfigProperty(name = PROP_PREFIX + "flowcontrol.max.outstanding.messages", defaultValue = "9223372036854775807")
    Long maxOutstandingMessages;

    @ConfigProperty(name = PROP_PREFIX + "flowcontrol.max.outstanding.bytes", defaultValue = "9223372036854775807")
    Long maxOutstandingRequestBytes;

    @ConfigProperty(name = PROP_PREFIX + "retry.total.timeout.ms", defaultValue = "60000")
    Integer maxTotalTimeoutMs;

    @ConfigProperty(name = PROP_PREFIX + "retry.max.rpc.timeout.ms", defaultValue = "10000")
    Integer maxRequestTimeoutMs;

    @ConfigProperty(name = PROP_PREFIX + "retry.initial.delay.ms", defaultValue = "5")
    Integer initialRetryDelay;

    @ConfigProperty(name = PROP_PREFIX + "retry.delay.multiplier", defaultValue = "2.0")
    Double retryDelayMultiplier;

    @ConfigProperty(name = PROP_PREFIX + "retry.max.delay.ms", defaultValue = "9223372036854775807")
    Long maxRetryDelay;

    @ConfigProperty(name = PROP_PREFIX + "retry.initial.rpc.timeout.ms", defaultValue = "10000")
    Integer initialRpcTimeout;

    @ConfigProperty(name = PROP_PREFIX + "retry.rpc.timeout.multiplier", defaultValue = "2.0")
    Double rpcTimeoutMultiplier;

    @ConfigProperty(name = PROP_PREFIX + "wait.message.delivery.timeout.ms", defaultValue = "30000")
    Integer waitMessageDeliveryTimeout;

    @ConfigProperty(name = PROP_PREFIX + "concurrency.threads", defaultValue = DEFAULT_CONCURRENCY_THREADS_STRING)
    int concurrencyThreads;

    @ConfigProperty(name = PROP_PREFIX + "compression.threshold.bytes", defaultValue = DEFAULT_COMPRESSION_THRESHOLD_BYTES_STRING)
    long compressionBytesThreshold;

    @ConfigProperty(name = PROP_PREFIX + "channel.shutdown.timeout.ms", defaultValue = "30000")
    Integer channelShutdownTimeout;

    @ConfigProperty(name = PROP_PREFIX + "address")
    Optional<String> address;

    @ConfigProperty(name = PROP_PREFIX + "region")
    Optional<String> region;

    @Inject
    @CustomConsumerBuilder
    Instance<PublisherBuilder> customPublisherBuilder;

    private ManagedChannel channel;
    private TransportChannelProvider channelProvider;
    private CredentialsProvider credentialsProvider;

    @PostConstruct
    void connect() {
        final Config config = ConfigProvider.getConfig();
        projectId = config.getOptionalValue(PROP_PROJECT_ID, String.class).orElse(ServiceOptions.getDefaultProjectId());

        if (customPublisherBuilder.isResolvable()) {
            publisherBuilder = customPublisherBuilder.get();
            LOGGER.info("Obtained custom configured PublisherBuilder '{}'", customPublisherBuilder);
            return;
        }

        BatchingSettings.Builder batchingSettings = BatchingSettings.newBuilder()
                .setDelayThreshold(Duration.ofMillis(maxDelayThresholdMs))
                .setElementCountThreshold(maxBufferSize)
                .setRequestByteThreshold(maxBufferBytes);

        if (flowControlEnabled) {
            batchingSettings.setFlowControlSettings(FlowControlSettings.newBuilder()
                    .setMaxOutstandingRequestBytes(maxOutstandingRequestBytes)
                    .setMaxOutstandingElementCount(maxOutstandingMessages)
                    .setLimitExceededBehavior(FlowController.LimitExceededBehavior.Block)
                    .build());
        }

        if (address.isPresent()) {
            String hostport = address.get();
            channel = ManagedChannelBuilder
                    .forTarget(hostport)
                    .usePlaintext()
                    .build();
            channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
            credentialsProvider = NoCredentialsProvider.create();
        }

        publisherBuilder = (t) -> {
            try {
                Builder builder = Publisher.newBuilder(t)
                        .setEnableMessageOrdering(orderingEnabled)
                        .setBatchingSettings(batchingSettings.build())
                        .setRetrySettings(
                                RetrySettings.newBuilder()
                                        .setTotalTimeout(Duration.ofMillis(maxTotalTimeoutMs))
                                        .setMaxRpcTimeout(Duration.ofMillis(maxRequestTimeoutMs))
                                        .setInitialRetryDelay(Duration.ofMillis(initialRetryDelay))
                                        .setRetryDelayMultiplier(retryDelayMultiplier)
                                        .setMaxRetryDelay(Duration.ofMillis(maxRetryDelay))
                                        .setInitialRpcTimeout(Duration.ofMillis(initialRpcTimeout))
                                        .setRpcTimeoutMultiplier(rpcTimeoutMultiplier)
                                        .build());

                if (concurrencyThreads > DEFAULT_CONCURRENCY_THREADS) {
                    builder.setExecutorProvider(
                            InstantiatingExecutorProvider.newBuilder()
                                    .setExecutorThreadCount(concurrencyThreads)
                                    .build());
                }

                if (compressionBytesThreshold >= DEFAULT_COMPRESSION_THRESHOLD_BYTES) {
                    builder.setEnableCompression(true)
                            .setCompressionBytesThreshold(compressionBytesThreshold);
                }

                if (address.isPresent()) {
                    builder.setChannelProvider(channelProvider).setCredentialsProvider(credentialsProvider);
                }
                else if (region.isPresent()) {
                    String endpoint = String.format("%s-pubsub.googleapis.com:443", region.get());
                    builder.setEndpoint(endpoint);
                }

                return builder.build();
            }
            catch (IOException e) {
                throw new DebeziumException(e);
            }
        };

        LOGGER.info("Using default PublisherBuilder '{}'", publisherBuilder);
    }

    @PreDestroy
    void close() {
        publishers.values().forEach(publisher -> {
            try {
                publisher.shutdown();
            }
            catch (Exception e) {
                LOGGER.warn("Exception while closing publisher: {}", e.getMessage(), e);
            }
        });
        shutdownChannel(channel);
    }

    void shutdownChannel(ManagedChannel channel) {

        if (channel != null && !channel.isShutdown()) {
            ExecutorService executor = Threads.newSingleThreadExecutor(this.getClass(), projectId, "managed-channel-shutdown-coordinator");

            Future<?> future = executor.submit(() -> {
                channel.shutdown();
                try {
                    channel.awaitTermination(channelShutdownTimeout, TimeUnit.MILLISECONDS);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            try {
                future.get(channelShutdownTimeout, TimeUnit.MILLISECONDS);
            }
            catch (Exception e) {
                LOGGER.warn("Exception while shutting down the managed channel {}", e.getMessage(), e);
            }
            finally {
                executor.shutdownNow();
            }
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {

        final List<ApiFuture<String>> deliveries = new ArrayList<>();

        for (ChangeEvent<Object, Object> record : records) {
            LOGGER.trace("Received event '{}'", record);
            final String topicName = streamNameMapper.map(record.destination());
            Publisher publisher = publishers.computeIfAbsent(topicName, (x) -> publisherBuilder.get(ProjectTopicName.of(projectId, x)));

            PubsubMessage message = buildPubSubMessage(record);

            deliveries.add(publisher.publish(message));
        }
        List<String> messageIds;
        try {
            messageIds = ApiFutures.allAsList(deliveries).get(waitMessageDeliveryTimeout, TimeUnit.MILLISECONDS);
        }
        catch (ExecutionException | TimeoutException e) {
            throw new DebeziumException(e);
        }
        LOGGER.trace("Sent messages with ids: {}", messageIds);

        // Once publishing is confirmed, mark all records as processed
        for (ChangeEvent<Object, Object> record : records) {
            committer.markProcessed(record);
        }

        committer.markBatchFinished();
    }

    private PubsubMessage buildPubSubMessage(ChangeEvent<Object, Object> record) {

        final PubsubMessage.Builder pubsubMessage = PubsubMessage.newBuilder();

        if (orderingEnabled) {
            if (orderingKey.isEmpty()) {
                if (record.key() == null) {
                    pubsubMessage.setOrderingKey(nullKey);
                }
                else if (record.key() instanceof String) {
                    pubsubMessage.setOrderingKey((String) record.key());
                }
                else if (record.key() instanceof byte[]) {
                    pubsubMessage.setOrderingKeyBytes(ByteString.copyFrom((byte[]) record.key()));
                }
            }
            else {
                pubsubMessage.setOrderingKey(orderingKey.get());
            }
        }

        if (record.value() instanceof String) {
            pubsubMessage.setData(ByteString.copyFromUtf8((String) record.value()));
        }
        else if (record.value() instanceof byte[]) {
            pubsubMessage.setData(ByteString.copyFrom((byte[]) record.value()));
        }

        pubsubMessage.putAllAttributes(convertHeaders(record));

        return pubsubMessage.build();
    }

    @Override
    public boolean supportsTombstoneEvents() {
        return false;
    }
}
