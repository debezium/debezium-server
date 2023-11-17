/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.sqs;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

import io.debezium.server.TestConfigSource;

/**
 * @author V K
 */
public class SqsTestConfigSource extends TestConfigSource {
    public SqsTestConfigSource() {
        Map<String, String> sqsConfigMap = new HashMap<>();

        sqsConfigMap.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        sqsConfigMap.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        sqsConfigMap.put("debezium.source.offset.flush.interval.ms", "0");
        sqsConfigMap.put("debezium.source.topic.prefix", "testc");
        sqsConfigMap.put("debezium.source.schema.include.list", "inventory");
        sqsConfigMap.put("debezium.source.table.include.list", "inventory.customers");

        config = sqsConfigMap;
    }

    @Override
    public int getOrdinal() {
        // Configuration property precedence is based on ordinal values and since we override the
        // properties in TestConfigSource, we should give this a higher priority.
        return super.getOrdinal() + 1;
    }

    public static int waitForSeconds() {
        return 60;
    }
}
