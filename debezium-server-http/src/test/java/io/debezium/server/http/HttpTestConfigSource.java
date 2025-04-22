/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.http;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

import io.debezium.embedded.async.ConvertingAsyncEngineBuilderFactory;
import io.debezium.server.TestConfigSource;

public class HttpTestConfigSource extends TestConfigSource {

    public HttpTestConfigSource() {
        Map<String, String> httpTest = new HashMap<>();

        httpTest.put("debezium.sink.type", "http");
        httpTest.put("debezium.format.value", "cloudevents"); // Need to explicitly pass in the cloudevents format

        httpTest.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        httpTest.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        httpTest.put("debezium.source.offset.flush.interval.ms", "0");
        httpTest.put("debezium.source.topic.prefix", "testc");
        httpTest.put("debezium.source.schema.include.list", "inventory");
        httpTest.put("debezium.source.table.include.list", "inventory.customers");
        httpTest.put("debezium.transforms", "addheader");
        httpTest.put("debezium.transforms.addheader.type", "org.apache.kafka.connect.transforms.InsertHeader");
        httpTest.put("debezium.transforms.addheader.header", "headerKey");
        httpTest.put("debezium.transforms.addheader.value.literal", "headerValue");
        httpTest.put("debezium.engine.factory", ConvertingAsyncEngineBuilderFactory.class.getName());

        config = httpTest;
    }

    @Override
    public int getOrdinal() {
        // Configuration property precedence is based on ordinal values and since we override the
        // properties in TestConfigSource, we should give this a higher priority.
        return super.getOrdinal() + 1;
    }
}
