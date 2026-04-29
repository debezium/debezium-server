/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.sqs;

import java.net.URI;
import java.time.Duration;
import java.util.List;

import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import io.debezium.server.api.DebeziumServerConsumer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.Module;
import io.debezium.config.Field;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataFactory;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.api.DebeziumServerSink;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest.Builder;

/**
 * Implementation of the consumer that delivers the messages into Amazon SQS destination.
 *
 * @author V K
 */
@Named("sqs")
@Dependent
public class SqsChangeConsumer extends BaseChangeConsumer implements DebeziumServerConsumer<CapturingEvents<BatchEvent>>, DebeziumServerSink {
    protected static final String PROP_PREFIX = "debezium.sink.sqs.";
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsChangeConsumer.class);
    private static final Duration RETRY_INTERVAL = Duration.ofSeconds(1);
    private static final int DEFAULT_RETRIES = 5;

    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();

    private SqsChangeConsumerConfig config;
    private String messageGroupId = null;
    private SqsClient client = null;

    @PostConstruct
    void connect() {
        final Config mpConfig = ConfigProvider.getConfig();

        // Load configuration
        io.debezium.config.Configuration configuration = io.debezium.config.Configuration.from(getConfigSubset(mpConfig, PROP_PREFIX));
        this.config = new SqsChangeConsumerConfig(configuration);

        final SqsClientBuilder builder = SqsClient.builder()
                .region(Region.of(config.getRegion()));

        if (config.getEndpoint() != null) {
            LOGGER.info("Queue Endpoint {}", config.getEndpoint());
            builder.endpointOverride(URI.create(config.getEndpoint()));
        }

        if (config.getCredentialsProfile() != null) {
            LOGGER.info("Credentials profile {}", config.getCredentialsProfile());
            builder.credentialsProvider(ProfileCredentialsProvider.create(config.getCredentialsProfile()));
        }

        client = builder.build();

        LOGGER.info("Queue Url {}", config.getQueueUrl());

        if (config.getQueueUrl().endsWith(".fifo")) {
            messageGroupId = config.getFifoMessageGroupId();
        }
    }

    @PreDestroy
    @Override
    public void close() {
        try {
            client.close();
        }
        catch (Exception e) {
            LOGGER.warn("Exception while closing Sqs client", e);
        }
    }

    @Override
    public void handle(CapturingEvents<BatchEvent> events)
            throws InterruptedException {
        for (BatchEvent record : events.records()) {
            LOGGER.trace("Received event '{}'", record);

            int attempts = 0;
            while (!recordSent(record, events.destination())) {
                attempts++;
                if (attempts >= DEFAULT_RETRIES) {
                    throw new DebeziumException("Exceeded maximum number of attempts to publish event " + record);
                }
                Metronome.sleeper(RETRY_INTERVAL, Clock.SYSTEM).pause();
            }
            record.commit();
        }
    }

    private boolean recordSent(BatchEvent event, String destination) {
        Object eventValue = event.value();
        if (eventValue == null) {
            eventValue = "";
        }

        LOGGER.info(event.toString());

        final Builder sendMessageRequestBuilder = SendMessageRequest.builder()
                .queueUrl(config.getQueueUrl())
                .messageBody(eventValue.toString());

        if (messageGroupId != null) {
            // At the moment this is a static group. If there is a need for more dynamic group id,
            // then we probably have to modify here by parsing the event and using a dynamic value based on the event payload.
            sendMessageRequestBuilder.messageGroupId(messageGroupId);
        }
        try {
            client.sendMessage(sendMessageRequestBuilder.build());
            return true;
        }
        catch (SdkClientException exception) {
            LOGGER.error("Failed to send record to {}", destination, exception);
            return false;
        }
    }

    @Override
    public Field.Set getConfigFields() {
        return Field.setOf(
                SqsChangeConsumerConfig.REGION,
                SqsChangeConsumerConfig.ENDPOINT,
                SqsChangeConsumerConfig.QUEUE_URL,
                SqsChangeConsumerConfig.CREDENTIALS_PROFILE,
                SqsChangeConsumerConfig.FIFO_MESSAGE_GROUP_ID);
    }

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(componentMetadataFactory.createComponentMetadata(this, Module.version()));
    }
}
