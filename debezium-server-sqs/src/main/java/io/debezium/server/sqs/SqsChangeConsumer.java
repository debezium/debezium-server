/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.sqs;

import java.net.URI;
import java.time.Duration;
import java.util.List;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine.ChangeConsumer;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.server.BaseChangeConsumer;
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
public class SqsChangeConsumer extends BaseChangeConsumer implements ChangeConsumer<ChangeEvent<Object, Object>> {
    protected static final String PROP_PREFIX = "debezium.sink.sqs.";
    protected static final String PROP_REGION_NAME = PROP_PREFIX + "region";
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsChangeConsumer.class);
    private static final Duration RETRY_INTERVAL = Duration.ofSeconds(1);
    private static final int DEFAULT_RETRIES = 5;
    private static final String PROP_ENDPOINT_NAME = PROP_PREFIX + "endpoint";
    private static final String PROP_QUEUE_URL = PROP_PREFIX + "queue.url";
    private static final String PROP_CREDENTIALS_PROFILE = PROP_PREFIX + "credentials.profile";
    private static final String PROP_QUEUE_FIFO_MESSAGE_GROUP_ID = PROP_PREFIX + "fifo.message.group.id";

    private String messageGroupId = null;
    private String queueUrl;
    private SqsClient client = null;

    @PostConstruct
    void connect() {
        final Config config = ConfigProvider.getConfig();
        final SqsClientBuilder builder = SqsClient.builder()
                .region(Region.of(config.getValue(PROP_REGION_NAME, String.class)));

        config.getOptionalValue(PROP_ENDPOINT_NAME, String.class).ifPresent(endpoint -> {
            LOGGER.info("Queue Endpoint {}", endpoint);
            builder.endpointOverride(URI.create(endpoint));
        });

        config.getOptionalValue(PROP_CREDENTIALS_PROFILE, String.class).ifPresent(profile -> {
            LOGGER.info("Credentials profile {}", profile);
            builder.credentialsProvider(ProfileCredentialsProvider.create(profile));
        });

        client = builder.build();

        queueUrl = config.getValue(PROP_QUEUE_URL, String.class);
        LOGGER.info("Queue Url {}", queueUrl);

        if (queueUrl.endsWith(".fifo")) {
            messageGroupId = config.getOptionalValue(PROP_QUEUE_FIFO_MESSAGE_GROUP_ID, String.class).orElse("cdc-group");
        }
    }

    @PreDestroy
    void close() {
        try {
            client.close();
        }
        catch (Exception e) {
            LOGGER.warn("Exception while closing Sqs client", e);
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        for (ChangeEvent<Object, Object> record : records) {
            LOGGER.trace("Received event '{}'", record);

            int attempts = 0;
            while (!recordSent(record)) {
                attempts++;
                if (attempts >= DEFAULT_RETRIES) {
                    throw new DebeziumException("Exceeded maximum number of attempts to publish event " + record);
                }
                Metronome.sleeper(RETRY_INTERVAL, Clock.SYSTEM).pause();
            }
            committer.markProcessed(record);
        }
        committer.markBatchFinished();
    }

    private boolean recordSent(ChangeEvent<Object, Object> event) {
        Object eventValue = event.value();
        if (eventValue == null) {
            eventValue = "";
        }

        LOGGER.info(event.toString());

        final Builder sendMessageRequestBuilder = SendMessageRequest.builder()
                .queueUrl(queueUrl)
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
            LOGGER.error("Failed to send record to {}", event.destination(), exception);
            return false;
        }
    }
}
