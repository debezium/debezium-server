/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.eventhubs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.azure.messaging.eventhubs.models.CreateBatchOptions;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;

public class BatchManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchManager.class);
    private final EventHubProducerClient producer;
    private final boolean forceSinglePartitionMode;
    private final String partitionID;
    private final String partitionKey;
    private final Integer maxBatchSize;

    // Prepare CreateBatchOptions for N partitions
    private final HashMap<Integer, CreateBatchOptions> batchOptions = new HashMap<>();
    private final HashMap<Integer, EventDataBatchProxy> batches = new HashMap<>();
    private final HashMap<Integer, ArrayList<Integer>> processedRecordIndices = new HashMap<>();
    private List<ChangeEvent<Object, Object>> records;
    private DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer;

    public BatchManager(EventHubProducerClient producer, boolean forceSinglePartitionMode,
                        String partitionID, String partitionKey, Integer maxBatchSize) {
        this.producer = producer;
        this.forceSinglePartitionMode = forceSinglePartitionMode;
        this.partitionID = partitionID;
        this.partitionKey = partitionKey;
        this.maxBatchSize = maxBatchSize;
    }

    public void initializeBatch(List<ChangeEvent<Object, Object>> records,
                                DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer) {
        this.records = records;
        this.committer = committer;

        if (forceSinglePartitionMode) {
            CreateBatchOptions op = new CreateBatchOptions();

            if (!partitionID.isEmpty()) {
                op.setPartitionId(partitionID);

                batchOptions.put(Integer.parseInt(partitionID), op);
                batches.put(Integer.parseInt(partitionID), new EventDataBatchProxy(producer, op));
                processedRecordIndices.put(Integer.parseInt(partitionID), new ArrayList<>());
            }

            if (!partitionKey.isEmpty()) {
                op.setPartitionKey(partitionKey);

                batchOptions.put(0, op);
                batches.put(0, new EventDataBatchProxy(producer, op));
                processedRecordIndices.put(0, new ArrayList<>());
            }

            if (maxBatchSize != 0) {
                op.setMaximumSizeInBytes(maxBatchSize);
            }

            return;
        }

        producer.getPartitionIds().stream().forEach(partitionId -> {
            CreateBatchOptions op = new CreateBatchOptions().setPartitionId(partitionId);
            if (maxBatchSize != 0) {
                op.setMaximumSizeInBytes(maxBatchSize);
            }
            batchOptions.put(Integer.parseInt(partitionId), op);
        });
        // Prepare batches
        batchOptions.forEach((partitionId, batchOption) -> {
            EventDataBatchProxy batch = new EventDataBatchProxy(producer, batchOption);
            batches.put(partitionId, batch);
            processedRecordIndices.put(partitionId, new ArrayList<>());
        });

    }

    public void closeAndEmitBatches() {
        // All records have been processed, emit the final (non-full) batches.
        batches.forEach((partitionId, batch) -> {
            if (batch.getCount() > 0) {
                LOGGER.trace("Dispatching {} events.", batch.getCount());
                emitBatchToEventHub(records, committer, processedRecordIndices.get(partitionId), batch);
            }
        });
    }

    public void sendEventToPartitionId(EventData eventData, Integer recordIndex, Integer partitionId) {
        EventDataBatchProxy batch = batches.get(partitionId);

        if (!batch.tryAdd(eventData)) {
            if (batch.getCount() == 0) {
                // If we fail to add at least the very first event to the batch that is because
                // the event's size exceeds the maxBatchSize in which case we cannot safely
                // recover and dispatch the event, only option is to throw an exception.
                throw new DebeziumException("Event data is too large to fit into batch");
            }
            // reached the maximum allowed size for the batch
            LOGGER.debug("Maximum batch size reached, dispatching {} events.", batch.getCount());

            // Max size reached, dispatch the batch to EventHub
            emitBatchToEventHub(records, committer, processedRecordIndices.get(partitionId), batch);
            // Renew the batch proxy so we can continue.
            batch = new EventDataBatchProxy(producer, batchOptions.get(partitionId));
            batches.put(partitionId, batch);
            processedRecordIndices.put(partitionId, new ArrayList<>());
        }

        // Record the index of the record that was added to the batch.
        processedRecordIndices.get(partitionId).add(recordIndex);
    }

    private void emitBatchToEventHub(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer,
                                     ArrayList<Integer> processedIndices, EventDataBatchProxy batch) {
        final int batchEventSize = batch.getCount();
        if (batchEventSize > 0) {
            try {
                LOGGER.trace("Sending batch of {} events to Event Hubs", batchEventSize);
                batch.emit();
                LOGGER.trace("Sent record batch to Event Hubs");
            }
            catch (Exception e) {
                throw new DebeziumException(e);
            }

            // this loop commits each record submitted in the event hubs batch
            List<String> processedIndexesStrings = processedIndices.stream().map(Object::toString).collect(Collectors.toList());
            LOGGER.trace("Marking records as processed: {}", String.join("; ", processedIndexesStrings));
            processedIndices.forEach(
                    index -> {
                        ChangeEvent<Object, Object> record = records.get(index);
                        try {
                            committer.markProcessed(record);
                            LOGGER.trace("Record marked processed");
                        }
                        catch (Exception e) {
                            throw new DebeziumException(e);
                        }
                    });
        }
    }
}
