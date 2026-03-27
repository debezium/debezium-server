/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.pubsub;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
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

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsublite.CloudRegionOrZone;
import com.google.cloud.pubsublite.ProjectId;
import com.google.cloud.pubsublite.TopicName;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.cloudpubsub.Publisher;
import com.google.cloud.pubsublite.cloudpubsub.PublisherSettings;
import com.google.protobuf.ByteString;
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

/**
 * Implementation of the consumer that delivers the messages into Google Pub/Sub Lite destination.
 */
@Named("pubsublite")
@Dependent
public class PubSubLiteChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>>, DebeziumServerSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(PubSubLiteChangeConsumer.class);

    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();

    private static final String PROP_PREFIX = "debezium.sink.pubsublite.";

    public interface PublisherBuilder {
        Publisher get(String topicName);
    }

    private PubSubLiteChangeConsumerConfig config;
    private PublisherBuilder publisherBuilder;
    private final Map<String, Publisher> publishers = new HashMap<>();

    @Inject
    @CustomConsumerBuilder
    Instance<PublisherBuilder> customPublisherBuilder;

    @PostConstruct
    void connect() {
        final Config mpConfig = ConfigProvider.getConfig();

        // Load configuration
        io.debezium.config.Configuration configuration = io.debezium.config.Configuration.from(getConfigSubset(mpConfig, PROP_PREFIX));
        this.config = new PubSubLiteChangeConsumerConfig(configuration);

        String projectId = (config.getProjectId() != null) ? config.getProjectId() : ServiceOptions.getDefaultProjectId();
        String region = config.getRegion();

        if (customPublisherBuilder.isResolvable()) {
            publisherBuilder = customPublisherBuilder.get();
            LOGGER.info("Obtained custom configured PublisherBuilder '{}'", customPublisherBuilder);
            return;
        }
        publisherBuilder = (t) -> {
            TopicPath topicPath = TopicPath
                    .newBuilder()
                    .setName(TopicName.of(t))
                    .setProject(ProjectId.of(projectId))
                    .setLocation(CloudRegionOrZone.parse(region))
                    .build();

            PublisherSettings publisherSettings = PublisherSettings
                    .newBuilder()
                    .setTopicPath(topicPath)
                    .build();
            Publisher publisher = Publisher.create(publisherSettings);
            publisher.startAsync().awaitRunning();
            return publisher;
        };
        LOGGER.info("Using default PublisherBuilder '{}'", publisherBuilder);
    }

    @PreDestroy
    @Override
    public void close() {
        publishers.values().forEach(publisher -> {
            try {
                publisher.stopAsync().awaitTerminated();
            }
            catch (Exception e) {
                LOGGER.warn("Exception while closing publisher: " + e);
            }
        });
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, RecordCommitter<ChangeEvent<Object, Object>> committer) throws InterruptedException {
        final List<ApiFuture<String>> deliveries = new ArrayList<>();
        for (ChangeEvent<Object, Object> record : records) {
            LOGGER.trace("Received event '{}'", record);
            final String topicName = streamNameMapper.map(record.destination());

            Publisher publisher = publishers.computeIfAbsent(topicName, (topic) -> publisherBuilder.get(topic));

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
                PubSubLiteChangeConsumerConfig.PROJECT_ID,
                PubSubLiteChangeConsumerConfig.REGION,
                PubSubLiteChangeConsumerConfig.ORDERING_ENABLED,
                PubSubLiteChangeConsumerConfig.NULL_KEY,
                PubSubLiteChangeConsumerConfig.WAIT_MESSAGE_DELIVERY_TIMEOUT_MS);
    }

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(componentMetadataFactory.createComponentMetadata(this, Module.version()));
    }
}
