/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.instructlab;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

import io.debezium.server.TestConfigSource;
import io.debezium.util.Testing;

/**
 * Defines the test configuration for the InstructLab integration tests.
 *
 * @author Chris Cranford
 */
public class InstructLabTestConfigSource extends TestConfigSource {

    public InstructLabTestConfigSource() {
        final Map<String, String> configMap = new HashMap<>();
        configMap.put("debezium.sink.type", "instructlab");

        configMap.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        configMap.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        configMap.put("debezium.source.offset.flush.interval.ms", "0");
        configMap.put("debezium.source.topic.prefix", "ilab");
        configMap.put("debezium.source.schema.include.list", "inventory");
        configMap.put("debezium.source.table.include.list", "inventory.orders");

        configMap.put("debezium.transforms", "fraud");
        configMap.put("debezium.transforms.fraud.type", FraudInstructLabAddQnaHeader.class.getName());
        configMap.put("debezium.transforms.fraud.name", "fraud");
        configMap.put("debezium.transforms.fraud.filename", Testing.Files.createTestingPath("fraud/qna.yml").toString());

        config = configMap;
    }

    @Override
    public int getOrdinal() {
        // Configuration property precedence is based on ordinal values and since we override the
        // properties in TestConfigSource, we should give this a higher priority.
        return super.getOrdinal() + 1;
    }
}
