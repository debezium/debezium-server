/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.eventhubs;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.azure.messaging.eventhubs.models.CreateBatchOptions;

/**
 * Proxy class/wrapper for EventDataBatch. Will create an inner EventDataBatch when data is being emitted to a specific
 * partition.
 */
public class EventDataBatchProxy {
    private EventDataBatch batch;
    private final EventHubProducerClient producer;
    private final CreateBatchOptions batchOptions;

    public EventDataBatchProxy(EventHubProducerClient producer, CreateBatchOptions batchOptions) {
        this.producer = producer;
        this.batchOptions = batchOptions;
    }

    public boolean tryAdd(final EventData eventData) {
        if (this.batch == null) {
            this.batch = producer.createBatch(this.batchOptions);
        }

        return batch.tryAdd(eventData);
    }

    public int getCount() {
        if (this.batch == null) {
            return 0;
        }

        return batch.getCount();
    }

    public void emit() {
        if (this.batch == null) {
            return;
        }

        producer.send(this.batch);
    }
}
