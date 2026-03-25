/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.sns;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

import io.debezium.server.TestConfigSource;

public class SnsTestConfigSource extends TestConfigSource {

    public static final String SNS_REGION = "us-east-1";

    public SnsTestConfigSource() {
        Map<String, String> snsConfigMap = new HashMap<>();

        snsConfigMap.put("debezium.sink.type", "sns");
        snsConfigMap.put("debezium.sink.sns.region", SNS_REGION);
        snsConfigMap.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        snsConfigMap.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        snsConfigMap.put("debezium.source.offset.flush.interval.ms", "0");
        snsConfigMap.put("debezium.source.topic.prefix", "testc");
        snsConfigMap.put("debezium.source.schema.include.list", "inventory");
        snsConfigMap.put("debezium.source.table.include.list", "inventory.customers");

        config = snsConfigMap;
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
