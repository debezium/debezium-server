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
import io.debezium.Module;
import io.debezium.config.Field;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataFactory;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.CustomConsumerBuilder;
import io.debezium.server.DebeziumServerSink;
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
public class PubSubChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>>, DebeziumServerSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(PubSubChangeConsumer.class);

    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();

    private static final String PROP_PREFIX = "debezium.sink.pubsub.";

    public interface PublisherBuilder {
        Publisher get(ProjectTopicName topicName);
    }

    private PubSubChangeConsumerConfig config;
    private String projectId;

    private final Map<String, Publisher> publishers = new HashMap<>();
    private PublisherBuilder publisherBuilder;

    @Inject
    @CustomConsumerBuilder
    Instance<PublisherBuilder> customPublisherBuilder;

    private ManagedChannel channel;
    private TransportChannelProvider channelProvider;
    private CredentialsProvider credentialsProvider;

    @PostConstruct
    void connect() {
        final Config mpConfig = ConfigProvider.getConfig();

        // Load configuration
        io.debezium.config.Configuration configuration = io.debezium.config.Configuration.from(getConfigSubset(mpConfig, PROP_PREFIX));
        this.config = new PubSubChangeConsumerConfig(configuration);

        projectId = (config.getProjectId() != null) ? config.getProjectId() : ServiceOptions.getDefaultProjectId();

        if (customPublisherBuilder.isResolvable()) {
            publisherBuilder = customPublisherBuilder.get();
            LOGGER.info("Obtained custom configured PublisherBuilder '{}'", customPublisherBuilder);
            return;
        }

        BatchingSettings.Builder batchingSettings = BatchingSettings.newBuilder()
                .setDelayThreshold(Duration.ofMillis(config.getMaxDelayThresholdMs()))
                .setElementCountThreshold(config.getMaxBufferSize())
                .setRequestByteThreshold(config.getMaxBufferBytes());

        if (config.isFlowControlEnabled()) {
            batchingSettings.setFlowControlSettings(FlowControlSettings.newBuilder()
                    .setMaxOutstandingRequestBytes(config.getMaxOutstandingRequestBytes())
                    .setMaxOutstandingElementCount(config.getMaxOutstandingMessages())
                    .setLimitExceededBehavior(FlowController.LimitExceededBehavior.Block)
                    .build());
        }

        if (config.getAddress() != null) {
            String hostport = config.getAddress();
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
                        .setEnableMessageOrdering(config.isOrderingEnabled())
                        .setBatchingSettings(batchingSettings.build())
                        .setRetrySettings(
                                RetrySettings.newBuilder()
                                        .setTotalTimeout(Duration.ofMillis(config.getMaxTotalTimeoutMs()))
                                        .setMaxRpcTimeout(Duration.ofMillis(config.getMaxRequestTimeoutMs()))
                                        .setInitialRetryDelay(Duration.ofMillis(config.getInitialRetryDelay()))
                                        .setRetryDelayMultiplier(config.getRetryDelayMultiplier())
                                        .setMaxRetryDelay(Duration.ofMillis(config.getMaxRetryDelay()))
                                        .setInitialRpcTimeout(Duration.ofMillis(config.getInitialRpcTimeout()))
                                        .setRpcTimeoutMultiplier(config.getRpcTimeoutMultiplier())
                                        .build());

                if (config.getConcurrencyThreads() > 0) {
                    builder.setExecutorProvider(
                            InstantiatingExecutorProvider.newBuilder()
                                    .setExecutorThreadCount(config.getConcurrencyThreads())
                                    .build());
                }

                if (config.getCompressionBytesThreshold() >= 0) {
                    builder.setEnableCompression(true)
                            .setCompressionBytesThreshold(config.getCompressionBytesThreshold());
                }

                if (config.getAddress() != null) {
                    builder.setChannelProvider(channelProvider).setCredentialsProvider(credentialsProvider);
                }
                else if (config.getRegion() != null) {
                    String endpoint = String.format("%s-pubsub.googleapis.com:443", config.getRegion());
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
    @Override
    public void close() {
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
                    channel.awaitTermination(config.getChannelShutdownTimeout(), TimeUnit.MILLISECONDS);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            try {
                future.get(config.getChannelShutdownTimeout(), TimeUnit.MILLISECONDS);
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
            messageIds = ApiFutures.allAsList(deliveries).get(config.getWaitMessageDeliveryTimeout(), TimeUnit.MILLISECONDS);
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

        if (config.isOrderingEnabled()) {
            if (config.getOrderingKey() == null) {
                if (record.key() == null) {
                    pubsubMessage.setOrderingKey(config.getNullKey());
                }
                else if (record.key() instanceof String) {
                    pubsubMessage.setOrderingKey((String) record.key());
                }
                else if (record.key() instanceof byte[]) {
                    pubsubMessage.setOrderingKeyBytes(ByteString.copyFrom((byte[]) record.key()));
                }
            }
            else {
                pubsubMessage.setOrderingKey(config.getOrderingKey());
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

    @Override
    public Field.Set getConfigFields() {
        return Field.setOf(
                PubSubChangeConsumerConfig.PROJECT_ID,
                PubSubChangeConsumerConfig.ORDERING_ENABLED,
                PubSubChangeConsumerConfig.ORDERING_KEY,
                PubSubChangeConsumerConfig.NULL_KEY,
                PubSubChangeConsumerConfig.BATCH_DELAY_THRESHOLD_MS,
                PubSubChangeConsumerConfig.BATCH_ELEMENT_COUNT_THRESHOLD,
                PubSubChangeConsumerConfig.BATCH_REQUEST_BYTE_THRESHOLD,
                PubSubChangeConsumerConfig.FLOWCONTROL_ENABLED,
                PubSubChangeConsumerConfig.FLOWCONTROL_MAX_OUTSTANDING_MESSAGES,
                PubSubChangeConsumerConfig.FLOWCONTROL_MAX_OUTSTANDING_BYTES,
                PubSubChangeConsumerConfig.RETRY_TOTAL_TIMEOUT_MS,
                PubSubChangeConsumerConfig.RETRY_MAX_RPC_TIMEOUT_MS,
                PubSubChangeConsumerConfig.RETRY_INITIAL_DELAY_MS,
                PubSubChangeConsumerConfig.RETRY_DELAY_MULTIPLIER,
                PubSubChangeConsumerConfig.RETRY_MAX_DELAY_MS,
                PubSubChangeConsumerConfig.RETRY_INITIAL_RPC_TIMEOUT_MS,
                PubSubChangeConsumerConfig.RETRY_RPC_TIMEOUT_MULTIPLIER,
                PubSubChangeConsumerConfig.WAIT_MESSAGE_DELIVERY_TIMEOUT_MS,
                PubSubChangeConsumerConfig.CONCURRENCY_THREADS,
                PubSubChangeConsumerConfig.COMPRESSION_THRESHOLD_BYTES,
                PubSubChangeConsumerConfig.CHANNEL_SHUTDOWN_TIMEOUT_MS,
                PubSubChangeConsumerConfig.ADDRESS,
                PubSubChangeConsumerConfig.REGION);
    }

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(componentMetadataFactory.createComponentMetadata(this, Module.version()));
    }
}
