/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.kinesis;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import io.debezium.server.api.DebeziumServerConsumer;
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
import io.debezium.server.api.DebeziumServerSink;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.KinesisClientBuilder;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

/**
 * Implementation of the consumer that delivers the messages into Amazon Kinesis destination.
 *
 * @author Jiri Pechanec
 *
 */
@Named("kinesis")
@Dependent
public class KinesisChangeConsumer extends BaseChangeConsumer implements DebeziumServerConsumer<CapturingEvents<BatchEvent>>, DebeziumServerSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisChangeConsumer.class);

    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();

    private static final String PROP_PREFIX = "debezium.sink.kinesis.";
    private static final Duration RETRY_INTERVAL = Duration.ofSeconds(1);

    private KinesisChangeConsumerConfig config;
    private KinesisClient client = null;

    @Inject
    @CustomConsumerBuilder
    Instance<KinesisClient> customClient;

    @PostConstruct
    void connect() {
        final Config mpConfig = ConfigProvider.getConfig();

        // Load configuration
        io.debezium.config.Configuration configuration = io.debezium.config.Configuration.from(getConfigSubset(mpConfig, PROP_PREFIX));
        this.config = new KinesisChangeConsumerConfig(configuration);

        if (config.getBatchSize() <= 0) {
            throw new DebeziumException("Batch size must be greater than 0");
        }
        else if (config.getBatchSize() > KinesisChangeConsumerConfig.MAX_BATCH_SIZE) {
            throw new DebeziumException("Batch size must be less than or equal to MAX_BATCH_SIZE");
        }

        if (customClient.isResolvable()) {
            client = customClient.get();
            LOGGER.info("Obtained custom configured KinesisClient '{}'", client);
            return;
        }

        final KinesisClientBuilder builder = KinesisClient.builder()
                .region(Region.of(config.getRegion()));

        if (config.getEndpoint() != null) {
            builder.endpointOverride(URI.create(config.getEndpoint()));
        }

        if (config.getCredentialsProfile() != null) {
            builder.credentialsProvider(ProfileCredentialsProvider.create(config.getCredentialsProfile()));
        }

        client = builder.build();
        LOGGER.info("Using default KinesisClient '{}'", client);
    }

    @PreDestroy
    @Override
    public void close() {
        try {
            client.close();
        }
        catch (Exception e) {
            LOGGER.warn("Exception while closing Kinesis client: {}", e);
        }
    }

    @Override
    public void handle(CapturingEvents<BatchEvent> events)
            throws InterruptedException {

        // Guard if records are empty
        if (events.records().isEmpty()) {
            return;
        }

        String streamName;
        List<BatchEvent> batch = new ArrayList<>();

        // Group the records by destination
        Map<String, List<BatchEvent>> segmentedBatches = events.records().stream().collect(Collectors.groupingBy(record -> events.destination()));

        // Iterate over the segmentedBatches
        for (Map.Entry<String, List<BatchEvent>> segmentedBatch : segmentedBatches.entrySet()) {
            // Iterate over the batch

            for (int i = 0; i < segmentedBatch.getValue().size(); i += config.getBatchSize()) {

                // Create a sublist of the batch given the batchSize
                batch = segmentedBatch.getValue().subList(i, Math.min(i + config.getBatchSize(), segmentedBatch.getValue().size()));
                List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<>();
                streamName = segmentedBatch.getKey();

                for (BatchEvent record : batch) {

                    Object rv = record.value();
                    if (rv == null) {
                        rv = "";
                    }
                    PutRecordsRequestEntry putRecordsRequestEntry = PutRecordsRequestEntry.builder()
                            .partitionKey((record.key() != null) ? getString(record.key()) : config.getNullKey())
                            .data(SdkBytes.fromByteArray(getBytes(rv))).build();
                    putRecordsRequestEntryList.add(putRecordsRequestEntry);
                }

                // Handle Error
                boolean notSuccesful = true;
                int attempts = 0;
                List<PutRecordsRequestEntry> batchRequest = putRecordsRequestEntryList;

                while (notSuccesful) {

                    if (attempts >= config.getMaxRetries()) {
                        throw new DebeziumException("Exceeded maximum number of attempts to publish event");
                    }

                    try {
                        PutRecordsResponse response = recordsSent(batchRequest, streamName);
                        attempts++;
                        if (response.failedRecordCount() > 0) {
                            LOGGER.warn("Failed to send {} number of records, retrying", response.failedRecordCount());
                            Metronome.sleeper(RETRY_INTERVAL, Clock.SYSTEM).pause();

                            final List<PutRecordsResultEntry> putRecordsResults = response.records();
                            List<PutRecordsRequestEntry> failedRecordsList = new ArrayList<>();

                            for (int index = 0; index < putRecordsResults.size(); index++) {
                                PutRecordsResultEntry entryResult = putRecordsResults.get(index);
                                if (entryResult.errorCode() != null) {
                                    failedRecordsList.add(batchRequest.get(index));
                                }
                            }
                            batchRequest = failedRecordsList;

                        }
                        else {
                            notSuccesful = false;
                            attempts = 0;
                        }

                    }
                    catch (KinesisException exception) {
                        LOGGER.warn("Failed to send record to {}", streamName, exception);
                        attempts++;
                        Metronome.sleeper(RETRY_INTERVAL, Clock.SYSTEM).pause();
                    }
                }

                for (BatchEvent record : batch) {
                    record.commit();
                }
            }
        }

    }

    private PutRecordsResponse recordsSent(List<PutRecordsRequestEntry> putRecordsRequestEntryList, String streamName) {

        // Create a PutRecordsRequest
        PutRecordsRequest putRecordsRequest = PutRecordsRequest.builder().streamName(streamNameMapper.map(streamName)).records(putRecordsRequestEntryList).build();

        // Send Request
        PutRecordsResponse putRecordsResponse = client.putRecords(putRecordsRequest);
        LOGGER.trace("Response Receieved: " + putRecordsResponse);
        return putRecordsResponse;
    }

    @Override
    public Field.Set getConfigFields() {
        return Field.setOf(
                KinesisChangeConsumerConfig.REGION,
                KinesisChangeConsumerConfig.ENDPOINT,
                KinesisChangeConsumerConfig.CREDENTIALS_PROFILE,
                KinesisChangeConsumerConfig.BATCH_SIZE,
                KinesisChangeConsumerConfig.DEFAULT_RETRIES,
                KinesisChangeConsumerConfig.NULL_KEY);
    }

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(componentMetadataFactory.createComponentMetadata(this, Module.version()));
    }
}
