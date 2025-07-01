/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.eventhubs;

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

    private String connectionString;
    private String eventHubName;
    private String configuredPartitionId;
    private String configuredPartitionKey;
    private Integer maxBatchSize;
    private Integer partitionCount;

    // connection string format -
    // Endpoint=sb://<NAMESPACE>/;SharedAccessKeyName=<KEY_NAME>;SharedAccessKey=<ACCESS_KEY>;EntityPath=<HUB_NAME>
    private static final String CONNECTION_STRING_FORMAT = "%s;EntityPath=%s";

    private EventHubProducerClient producer = null;
    private BatchManager batchManager = null;

    @Inject
    @CustomConsumerBuilder
    Instance<EventHubProducerClient> customProducer;

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

        // optional config
        maxBatchSize = config.getOptionalValue(PROP_MAX_BATCH_SIZE, Integer.class).orElse(0);
        configuredPartitionId = config.getOptionalValue(PROP_PARTITION_ID, String.class).orElse("");
        configuredPartitionKey = config.getOptionalValue(PROP_PARTITION_KEY, String.class).orElse("");

        String finalConnectionString = String.format(CONNECTION_STRING_FORMAT, connectionString, eventHubName);

        try {
            producer = new EventHubClientBuilder().connectionString(finalConnectionString).buildProducerClient();
            batchManager = new BatchManager(producer, configuredPartitionId, configuredPartitionKey, maxBatchSize);
        }
        catch (Exception e) {
            throw new DebeziumException(e);
        }

        LOGGER.info("Using default Event Hubs client for namespace '{}'", producer.getFullyQualifiedNamespace());

        // Retrieve available partition count for the EventHub
        partitionCount = (int) producer.getPartitionIds().stream().count();
        LOGGER.trace("Event Hub '{}' has {} partitions available", producer.getEventHubName(), partitionCount);

        if (!configuredPartitionId.isEmpty() && Integer.parseInt(configuredPartitionId) > partitionCount - 1) {
            throw new IndexOutOfBoundsException(
                    String.format("Target partition id %s does not exist in target EventHub %s", configuredPartitionId, eventHubName));
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

        batchManager.initializeBatch();

        for (int recordIndex = 0; recordIndex < records.size();) {
            int start = recordIndex;
            LOGGER.trace("Emitting events starting from index {}", start);

            // The inner loop adds as many records to the batch as possible, keeping track of the batch size
            for (; recordIndex < records.size(); recordIndex++) {
                ChangeEvent<Object, Object> record = records.get(recordIndex);

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

                // Find the partition to send eventData to.
                Integer targetPartitionId;
                String dynamicPartitionKey = null;

                if (!configuredPartitionId.isEmpty()) {
                    targetPartitionId = Integer.parseInt(configuredPartitionId);
                }
                else if (!configuredPartitionKey.isEmpty()) {
                    // The BatchManager
                    targetPartitionId = BatchManager.BATCH_INDEX_FOR_PARTITION_KEY;
                }
                else {
                    if (record.key() != null) {
                        dynamicPartitionKey = getString(record.key());
                        targetPartitionId = null;
                    }
                    else {
                        targetPartitionId = record.partition();
                        if (targetPartitionId == null) {
                            targetPartitionId = BatchManager.BATCH_INDEX_FOR_NO_PARTITION_ID;
                        }
                    }
                }

                try {
                    if (dynamicPartitionKey != null) {
                        batchManager.sendEventWithDynamicPartitionKey(eventData, dynamicPartitionKey);
                    }
                    else {
                        // Check that the target partition exists.
                        if (targetPartitionId < BatchManager.BATCH_INDEX_FOR_NO_PARTITION_ID || targetPartitionId > partitionCount - 1) {
                            throw new IndexOutOfBoundsException(
                                    String.format("Target partition id %d does not exist in target EventHub %s", targetPartitionId, eventHubName));
                        }
                        
                        batchManager.sendEventToPartitionId(eventData, recordIndex, targetPartitionId);
                    }
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

        LOGGER.trace("Marking {} records as processed.", records.size());
        for (ChangeEvent<Object, Object> record : records) {
            committer.markProcessed(record);
        }
        committer.markBatchFinished();
        LOGGER.trace("Batch marked finished");
    }
}
