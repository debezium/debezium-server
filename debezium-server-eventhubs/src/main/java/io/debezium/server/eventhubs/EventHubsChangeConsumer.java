/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.eventhubs;

import java.util.Arrays;
import java.util.List;

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

import com.azure.core.amqp.exception.AmqpException;
import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.CustomConsumerBuilder;

/**
 * This sink adapter delivers change event messages to Azure Event Hubs
 *
 * @author Abhishek Gupta
 */
@Named("eventhubs")
@Dependent
public class EventHubsChangeConsumer extends BaseChangeConsumer
        implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventHubsChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.eventhubs.";
    private static final String PROP_CONNECTION_STRING_NAME = PROP_PREFIX + "connectionstring";
    private static final String PROP_EVENTHUB_NAME = PROP_PREFIX + "hubname";
    private static final String PROP_PARTITION_ID = PROP_PREFIX + "partitionid";
    private static final String PROP_PARTITION_KEY = PROP_PREFIX + "partitionkey";
    // maximum size for the batch of events (bytes)
    private static final String PROP_MAX_BATCH_SIZE = PROP_PREFIX + "maxbatchsize";

    // Supports Struct nesting using dot notation.
    public static final String PROP_PARTITIONING_SELECTOR = PROP_PREFIX + "partitioning.selector";
    private static final List<String> PARTITIONING_SELECTOR_OPTIONS = Arrays.asList("destination", "key", "value");

    public static final String PROP_PARTITIONING_FIELD = PROP_PREFIX + "partitioning.field";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private String connectionString;
    private String eventHubName;
    private String partitionID;
    private String partitionKey;
    private Integer maxBatchSize;
    private Integer partitionCount;

    // connection string format -
    // Endpoint=sb://<NAMESPACE>/;SharedAccessKeyName=<KEY_NAME>;SharedAccessKey=<ACCESS_KEY>;EntityPath=<HUB_NAME>
    private static final String CONNECTION_STRING_FORMAT = "%s;EntityPath=%s";

    private EventHubProducerClient producer = null;
    private EventHubsPartitionKeyCalculator partitionKeyCalculator = null;

    private BatchManager batchManager = null;

    @Inject
    @CustomConsumerBuilder
    Instance<EventHubProducerClient> customProducer;

    @Inject
    Instance<EventHubsPartitionKeyCalculator> customPartitionKeyCalculator;
    private boolean forceSinglePartitionMode = false;

    @PostConstruct
    void connect() {
        if (customProducer.isResolvable()) {
            producer = customProducer.get();
            LOGGER.info("Obtained custom configured Event Hubs client for namespace '{}'",
                    customProducer.get().getFullyQualifiedNamespace());
            return;
        }

        final Config config = ConfigProvider.getConfig();
        connectionString = config.getValue(PROP_CONNECTION_STRING_NAME, String.class);
        eventHubName = config.getValue(PROP_EVENTHUB_NAME, String.class);

        maxBatchSize = config.getOptionalValue(PROP_MAX_BATCH_SIZE, Integer.class).orElse(0);

        configurePartitioningOptions(config);

        String finalConnectionString = String.format(CONNECTION_STRING_FORMAT, connectionString, eventHubName);

        try {
            producer = new EventHubClientBuilder().connectionString(finalConnectionString).buildProducerClient();
            batchManager = new BatchManager(producer, forceSinglePartitionMode, partitionID, partitionKey, maxBatchSize);
        }
        catch (Exception e) {
            throw new DebeziumException(e);
        }

        LOGGER.info("Using default Event Hubs client for namespace '{}'", producer.getFullyQualifiedNamespace());

        // Retrieve available partition count for the EventHub
        partitionCount = (int) producer.getPartitionIds().stream().count();
        LOGGER.warn("Event Hub '{}' has {} partitions", producer.getEventHubName(), partitionCount);
    }

    private void configurePartitioningOptions(Config config) {
        // optional config
        partitionID = config.getOptionalValue(PROP_PARTITION_ID, String.class).orElse("");
        partitionKey = config.getOptionalValue(PROP_PARTITION_KEY, String.class).orElse("");
        LOGGER.trace("Using partitionID {} and partitionKey {}", partitionID, partitionKey);
        if (partitionID != "" || partitionKey != "") {
            forceSinglePartitionMode = true;
            LOGGER.trace("Using single partition mode for Event Hub '{}' with partitionID {} and partitionKey {}", eventHubName, partitionID, partitionKey);
        }

        String partitioningSelector = config.getOptionalValue(PROP_PARTITIONING_SELECTOR, String.class).orElse("");
        String partitioningField = config.getOptionalValue(PROP_PARTITIONING_FIELD, String.class).orElse("");
        if (partitioningSelector != "" && !PARTITIONING_SELECTOR_OPTIONS.contains(partitioningSelector)) {
            throw new DebeziumException("Invalid value for " + PROP_PARTITIONING_SELECTOR + " property: " + partitioningSelector);
        }
        partitionKeyCalculator = new EventHubsDefaultPartitionKeyCalculatorImpl(partitioningSelector, partitioningField);

        if (customPartitionKeyCalculator.isResolvable()) {
            partitionKeyCalculator = customPartitionKeyCalculator.get();
            LOGGER.info("Obtained custom Event Hubs partition key calculator '{}'",
                    customPartitionKeyCalculator.get().getClass().getName());
        }
        else {
            partitionKeyCalculator = new EventHubsDefaultPartitionKeyCalculatorImpl(partitioningSelector, partitioningField);
            LOGGER.info("Using default Event Hubs partition key calculator '{}'",
                    partitionKeyCalculator.getClass().getName());
        }
    }

    @PreDestroy
    void close() {
        try {
            producer.close();
            LOGGER.info("Closed Event Hubs producer client");
        }
        catch (Exception e) {
            LOGGER.warn("Exception while closing Event Hubs producer: {}", e);
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records,
                            RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        LOGGER.trace("Event Hubs sink adapter processing change events");

        batchManager.initializeBatch(records, committer);

        for (int recordIndex = 0; recordIndex < records.size();) {
            int start = recordIndex;
            LOGGER.trace("Emitting events starting from index {}", start);

            // The inner loop adds as many records to the batch as possible, keeping track of the batch size
            for (; recordIndex < records.size(); recordIndex++) {
                ChangeEvent<Object, Object> record = records.get(recordIndex);
                LOGGER.trace("Received record with destination '{}'", record.destination());
                LOGGER.trace("Received record with key '{}'", record.key());
                LOGGER.trace("Received record with value '{}'", record.value());
                if (null == record.value()) {
                    continue;
                }

                EventData eventData;
                if (record.value() instanceof String) {
                    eventData = new EventData((String) record.value());
                }
                else if (record.value() instanceof byte[]) {
                    eventData = new EventData(getBytes(record.value()));
                }
                else {
                    LOGGER.warn("Event data in record.value() is not of type String or byte[]");

                    continue;
                }

                // Derive the partition to send eventData to from the record.value().
                Integer partitionId;
                if (forceSinglePartitionMode) {
                    partitionId = Integer.parseInt(partitionID);
                }
                else {
                    partitionId = this.partitionKeyCalculator.derivePartitionIdFromRecord(record, partitionCount);
                }

                try {
                    batchManager.sendEventToPartitionId(eventData, recordIndex, partitionId);
                }
                catch (IllegalArgumentException e) {
                    // thrown by tryAdd if event data is null
                    throw new DebeziumException(e);
                }
                catch (AmqpException e) {
                    // tryAdd throws AmqpException if "eventData is larger than the maximum size of
                    // the EventDataBatch."
                    throw new DebeziumException("Event data was larger than the maximum size of the batch", e);
                }
                catch (Exception e) {
                    throw new DebeziumException(e);
                }
            }
        }

        batchManager.closeAndEmitBatches();
        committer.markBatchFinished();
        LOGGER.trace("Batch marked finished");
    }
}
