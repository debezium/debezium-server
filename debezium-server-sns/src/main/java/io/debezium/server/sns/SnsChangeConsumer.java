/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.sns;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.CustomConsumerBuilder;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.SnsClientBuilder;
import software.amazon.awssdk.services.sns.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;
import software.amazon.awssdk.services.sns.model.PublishBatchRequest;
import software.amazon.awssdk.services.sns.model.PublishBatchRequestEntry;
import software.amazon.awssdk.services.sns.model.PublishBatchResponse;
import software.amazon.awssdk.services.sns.model.SnsException;

/**
 * Implementation of the consumer that delivers the messages into Amazon SNS destination.
 *
 * @author Rafael Rain
 */
@Named("sns")
@Dependent
public class SnsChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnsChangeConsumer.class);

    static final String PROP_PREFIX = "debezium.sink.sns.";
    private static final String PROP_REGION_NAME = PROP_PREFIX + "region";
    private static final String PROP_ENDPOINT_NAME = PROP_PREFIX + "endpoint";
    private static final String PROP_CREDENTIALS_PROFILE = PROP_PREFIX + "credentials.profile";
    private static final String PROP_TOPIC_ARN = PROP_PREFIX + "topic.arn";
    private static final String PROP_TOPIC_ARN_PREFIX = PROP_PREFIX + "topic.arn.prefix";
    private static final String PROP_RETRIES = PROP_PREFIX + "default.retries";
    private static final String PROP_MESSAGE_GROUP_ID_HEADER = PROP_PREFIX + "fifo.message.group.id.header";
    private static final String PROP_MESSAGE_DEDUP_ID_HEADER = PROP_PREFIX + "fifo.message.dedup.id.header";
    private static final String PROP_FIFO_DEFAULT_GROUP_ID = PROP_PREFIX + "fifo.default.group.id";

    static final int MAX_BATCH_SIZE = 10;
    static final int DEFAULT_RETRY_COUNT = 5;
    static final int MAX_SNS_MESSAGE_BYTES = 256 * 1024;
    private static final Duration RETRY_INTERVAL = Duration.ofSeconds(1);

    private String defaultTopicArn;
    private String topicArnPrefix;
    private int maxRetries;
    private String messageGroupIdHeader;
    private String messageDeduplicationIdHeader;
    private String fifoDefaultGroupId;

    private SnsClient client = null;

    @Inject
    @CustomConsumerBuilder
    Instance<SnsClient> customClient;

    @PostConstruct
    void connect() {
        final Config config = ConfigProvider.getConfig();
        maxRetries = config.getOptionalValue(PROP_RETRIES, Integer.class).orElse(DEFAULT_RETRY_COUNT);

        if (customClient.isResolvable()) {
            client = customClient.get();
            LOGGER.info("Obtained custom configured SnsClient '{}'", client);
        }
        else {
            final SnsClientBuilder builder = SnsClient.builder()
                    .region(Region.of(config.getValue(PROP_REGION_NAME, String.class)));

            config.getOptionalValue(PROP_ENDPOINT_NAME, String.class).ifPresent(endpoint -> {
                LOGGER.info("SNS Endpoint override: {}", endpoint);
                builder.endpointOverride(URI.create(endpoint));
            });

            config.getOptionalValue(PROP_CREDENTIALS_PROFILE, String.class).ifPresent(profile -> {
                LOGGER.info("Using credentials profile: {}", profile);
                builder.credentialsProvider(ProfileCredentialsProvider.create(profile));
            });

            client = builder.build();
        }

        topicArnPrefix = config.getOptionalValue(PROP_TOPIC_ARN_PREFIX, String.class).orElse(null);
        defaultTopicArn = config.getOptionalValue(PROP_TOPIC_ARN, String.class).orElse(null);
        messageGroupIdHeader = config.getOptionalValue(PROP_MESSAGE_GROUP_ID_HEADER, String.class).orElse("aggregateId");
        messageDeduplicationIdHeader = config.getOptionalValue(PROP_MESSAGE_DEDUP_ID_HEADER, String.class).orElse(null);
        fifoDefaultGroupId = config.getOptionalValue(PROP_FIFO_DEFAULT_GROUP_ID, String.class).orElse("default");

        LOGGER.info("Using topic ARN prefix: '{}', default topic ARN: '{}'", topicArnPrefix, defaultTopicArn);
    }

    @PreDestroy
    void close() {
        try {
            if (client != null) {
                client.close();
            }
        }
        catch (Exception e) {
            LOGGER.warn("Exception while closing SNS client", e);
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {

        if (records.isEmpty()) {
            committer.markBatchFinished();
            return;
        }

        // Group by destination (mapped topic ARN)
        Map<String, List<ChangeEvent<Object, Object>>> groupedByDestination = records.stream()
                .collect(Collectors.groupingBy(record -> resolveTopicArn(record.destination())));

        for (List<ChangeEvent<Object, Object>> destinationBatch : groupedByDestination.values()) {
            for (int i = 0; i < destinationBatch.size(); i += MAX_BATCH_SIZE) {
                List<ChangeEvent<Object, Object>> batch = destinationBatch.subList(i, Math.min(i + MAX_BATCH_SIZE, destinationBatch.size()));
                String topicArn = resolveTopicArn(batch.getFirst().destination());
                boolean isFifo = topicArn.endsWith(".fifo");

                List<PublishBatchRequestEntry> entries = new ArrayList<>(batch.size());
                for (int j = 0; j < batch.size(); j++) {
                    entries.add(buildEntry(batch.get(j), String.valueOf(i + j), isFifo));
                }

                sendBatchWithRetry(entries, topicArn);

                for (ChangeEvent<Object, Object> record : batch) {
                    committer.markProcessed(record);
                }
            }
        }

        committer.markBatchFinished();
    }

    private void sendBatchWithRetry(List<PublishBatchRequestEntry> entries, String topicArn) throws InterruptedException {
        int attempts = 0;
        List<PublishBatchRequestEntry> pending = entries;

        while (!pending.isEmpty()) {
            if (attempts >= maxRetries) {
                throw new DebeziumException("Exceeded maximum number of attempts (" + maxRetries + ") to publish batch to " + topicArn);
            }

            try {
                PublishBatchResponse response = client.publishBatch(
                        PublishBatchRequest.builder()
                                .topicArn(topicArn)
                                .publishBatchRequestEntries(pending)
                                .build());

                if (response.hasFailed() && !response.failed().isEmpty()) {
                    LOGGER.warn("Failed to publish {} entries to {}, retrying", response.failed().size(), topicArn);
                    var failedIds = response.failed().stream()
                            .map(BatchResultErrorEntry::id)
                            .collect(Collectors.toSet());
                    pending = pending.stream()
                            .filter(e -> failedIds.contains(e.id()))
                            .collect(Collectors.toList());
                    attempts++;
                    Metronome.sleeper(RETRY_INTERVAL, Clock.SYSTEM).pause();
                }
                else {
                    // All succeeded
                    pending = List.of();
                }
            }
            catch (SnsException exception) {
                LOGGER.warn("SNS exception while publishing to {}", topicArn, exception);
                attempts++;
                if (attempts >= maxRetries) {
                    throw new DebeziumException("Exceeded maximum number of attempts (" + maxRetries + ") to publish batch to " + topicArn, exception);
                }
                Metronome.sleeper(RETRY_INTERVAL, Clock.SYSTEM).pause();
            }
        }
    }

    private PublishBatchRequestEntry buildEntry(ChangeEvent<Object, Object> event, String batchEntryId, boolean isFifo) {
        String messageBody = getMessageBody(event);
        validatePayloadSize(messageBody, event.destination());

        PublishBatchRequestEntry.Builder builder = PublishBatchRequestEntry.builder()
                .id(batchEntryId)
                .message(messageBody);

        // Forward headers as MessageAttributes
        Map<String, String> headers = extractRawHeaders(event);
        if (!headers.isEmpty()) {
            Map<String, MessageAttributeValue> attributes = headers.entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> MessageAttributeValue.builder()
                                    .dataType("String")
                                    .stringValue(e.getValue())
                                    .build()));
            builder.messageAttributes(attributes);
        }

        if (isFifo) {
            // MessageGroupId: use header value if present, fallback to event key, then default
            String groupId = headers.getOrDefault(messageGroupIdHeader,
                    event.key() != null ? getString(event.key()) : fifoDefaultGroupId);
            builder.messageGroupId(groupId);

            // MessageDeduplicationId: use header if configured and present
            if (messageDeduplicationIdHeader != null) {
                String dedupId = headers.get(messageDeduplicationIdHeader);
                if (dedupId != null) {
                    builder.messageDeduplicationId(dedupId);
                }
            }
        }

        return builder.build();
    }

    /**
     * Extracts the message body from the raw Connect value, bypassing the converter.
     *
     * <p>The Outbox Event Router produces a STRING Connect value containing the JSON payload.
     * Using the raw {@link SourceRecord} value avoids the {@code JsonConverter} serialization
     * that would otherwise wrap it in a {@code {"schema":...,"payload":...}} envelope.
     *
     * <p>For non-string Connect types (e.g., Struct from CDC without the outbox SMT),
     * falls back to the converter-serialized output.
     */
    private String getMessageBody(ChangeEvent<Object, Object> event) {
        SourceRecord sourceRecord = toSourceRecord(event);
        Object rawValue = sourceRecord.value();
        if (rawValue == null) {
            return "";
        }
        if (rawValue instanceof String s) {
            return s;
        }
        // Non-string Connect type, fall back to the converter-serialized output
        return asString(event.value());
    }

    /**
     * Extracts raw Connect header values from the {@link SourceRecord}, bypassing the converter.
     *
     * <p>The inherited {@code convertHeaders} reads from the {@link ChangeEvent}, where headers
     * are already serialized by the header converter (e.g., {@code JsonConverter} wrapping each
     * value in {@code {"schema":...,"payload":"..."}}). This method reads directly from the
     * {@link SourceRecord}, where header values are still plain Java objects
     * (typically Strings for the Outbox Event Router).
     */
    private Map<String, String> extractRawHeaders(ChangeEvent<Object, Object> record) {
        SourceRecord sourceRecord = toSourceRecord(record);
        Map<String, String> result = new HashMap<>();
        for (Header header : sourceRecord.headers()) {
            Object value = header.value();
            result.put(header.key(), value != null ? value.toString() : "");
        }
        return result;
    }

    /**
     * Accesses the Kafka Connect {@link SourceRecord} from the engine's change event.
     *
     * <p>This is not part of the public Debezium Engine API but is an implementation detail
     * on which Debezium Server can rely (same pattern used by the Qdrant sink).
     */
    @SuppressWarnings("rawtypes")
    private SourceRecord toSourceRecord(ChangeEvent<Object, Object> record) {
        return ((EmbeddedEngineChangeEvent) record).sourceRecord();
    }

    private void validatePayloadSize(String messageBody, String destination) {
        int size = messageBody.getBytes(StandardCharsets.UTF_8).length;
        if (size > MAX_SNS_MESSAGE_BYTES) {
            throw new DebeziumException(
                    "Message payload size (" + size + " bytes) exceeds SNS limit of " + MAX_SNS_MESSAGE_BYTES
                            + " bytes for destination " + destination);
        }
    }

    String resolveTopicArn(String destination) {
        String mapped = streamNameMapper.map(destination);
        // If the mapped name looks like a full ARN, use it directly
        if (mapped.startsWith("arn:")) {
            return mapped;
        }
        // If a prefix is configured, compose the ARN from prefix + destination
        if (topicArnPrefix != null) {
            return topicArnPrefix + mapped;
        }
        // Otherwise, use default topic ARN if configured
        if (defaultTopicArn != null) {
            return defaultTopicArn;
        }
        // If no default and not a full ARN, treat mapped value as the ARN
        return mapped;
    }
}
