/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.rabbitmq;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

import io.debezium.server.TestConfigSource;

public class RabbitMqTestConfigSource extends TestConfigSource {

    public static final String TOPIC_NAME = "inventory-customers";

    public RabbitMqTestConfigSource() {

        final Map<String, String> rabbitmqConfig = new HashMap<>();
        rabbitmqConfig.put("debezium.sink.type", "rabbitmq");
        rabbitmqConfig.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        rabbitmqConfig.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        rabbitmqConfig.put("debezium.source.offset.flush.interval.ms", "0");
        rabbitmqConfig.put("debezium.source.topic.prefix", "testc");
        rabbitmqConfig.put("debezium.source.schema.include.list", "inventory");
        rabbitmqConfig.put("debezium.source.table.include.list", "inventory.customers");
        rabbitmqConfig.put("debezium.transforms", "Reroute,addheader");
        rabbitmqConfig.put("debezium.transforms.Reroute.type", "io.debezium.transforms.ByLogicalTableRouter");
        rabbitmqConfig.put("debezium.transforms.Reroute.topic.regex", "(.*)");
        rabbitmqConfig.put("debezium.transforms.Reroute.topic.replacement", TOPIC_NAME);
        rabbitmqConfig.put("debezium.transforms.addheader.type", "org.apache.kafka.connect.transforms.InsertHeader");
        rabbitmqConfig.put("debezium.transforms.addheader.header", "headerKey");
        rabbitmqConfig.put("debezium.transforms.addheader.value.literal", "headerValue");
        config = rabbitmqConfig;
    }

    @Override
    public int getOrdinal() {
        // Configuration property precedence is based on ordinal values and since we override the
        // properties in TestConfigSource, we should give this a higher priority.
        return super.getOrdinal() + 1;
    }
}
