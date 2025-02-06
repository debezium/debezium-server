/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.milvus;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

import io.debezium.server.TestConfigSource;

public class MilvusTestConfigSource extends TestConfigSource {

    public MilvusTestConfigSource() {
        Map<String, String> milvusTest = new HashMap<>();

        milvusTest.put("debezium.sink.type", "milvus");
        milvusTest.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        milvusTest.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                OFFSET_STORE_PATH.toAbsolutePath().toString());
        milvusTest.put("debezium.source.offset.flush.interval.ms", "0");
        milvusTest.put("debezium.source.topic.prefix", "testc");
        milvusTest.put("debezium.source.schema.include.list", "inventory");
        milvusTest.put("debezium.source.table.include.list", "inventory.t_vector");

        config = milvusTest;
    }

    @Override
    public int getOrdinal() {
        // Configuration property precedence is based on ordinal values and since we override the
        // properties in TestConfigSource, we should give this a higher priority.
        return super.getOrdinal() + 1;
    }
}
