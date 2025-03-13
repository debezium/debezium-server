/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.instructlab;

import java.util.Optional;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;

import io.debezium.server.instructlab.transforms.AbstractAddQnaHeader;

/**
 * @author Chris Cranford
 */
public class FraudInstructLabAddQnaHeader<R extends ConnectRecord<R>> extends AbstractAddQnaHeader<R> {
    @Override
    protected Optional<QnaEntry> createQnaEntry(R record) {
        if (record.topic().equals("ilab.inventory.orders")) {
            if (getAfter(record).getInt32("quantity") > 1) {
                final int orderId = getAfter(record).getInt32("id");
                return Optional.of(new QnaEntry(
                        "Is order " + orderId + " potentially fraudulent?",
                        "Yes, the order's quantity is greater than 1, which is the maximum allowed."));
            }
        }
        return Optional.empty();
    }

    @Override
    public String version() {
        return "1";
    }

    @Override
    public ConfigDef config() {
        return null;
    }

    @Override
    public void close() {

    }
}
