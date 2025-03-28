/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.instructlab;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.transforms.HeaderFrom;

import io.debezium.server.TestConfigSource;
import io.debezium.transforms.ExtractNewRecordState;
import io.debezium.util.Testing;

/**
 * Defines the test configuration for the InstructLab integration tests.
 *
 * @author Chris Cranford
 */
public class InstructLabTestConfigSource extends TestConfigSource {

    public InstructLabTestConfigSource() {
        final Path taxonomyPath = Testing.Files.createTestingPath("taxonomies");

        final Map<String, String> configMap = new HashMap<>();
        configMap.put("debezium.sink.type", "instructlab");
        configMap.put("debezium.sink.instructlab.taxonomy.base.path", taxonomyPath.toAbsolutePath().toString());
        configMap.put("debezium.sink.instructlab.taxonomies", "t1,t2");
        configMap.put("debezium.sink.instructlab.taxonomy.t1.topic", ".*");
        configMap.put("debezium.sink.instructlab.taxonomy.t1.question", "header:question");
        configMap.put("debezium.sink.instructlab.taxonomy.t1.answer", "header:answer");
        configMap.put("debezium.sink.instructlab.taxonomy.t1.context", "header:context");
        configMap.put("debezium.sink.instructlab.taxonomy.t1.domain", "t1/a/b/c/");
        configMap.put("debezium.sink.instructlab.taxonomy.t2.topic", "ilab\\.inventory\\.orders");
        configMap.put("debezium.sink.instructlab.taxonomy.t2.question", "value:purchaser");
        configMap.put("debezium.sink.instructlab.taxonomy.t2.answer", "value:product_id");
        configMap.put("debezium.sink.instructlab.taxonomy.t2.domain", "t2/x/y/z/");

        configMap.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        configMap.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        configMap.put("debezium.source.offset.flush.interval.ms", "0");
        configMap.put("debezium.source.topic.prefix", "ilab");
        configMap.put("debezium.source.schema.include.list", "inventory");
        configMap.put("debezium.source.table.include.list", "inventory.orders");

        // Copies field's "question", "answer", and "context" to headers of the same names
        // The headers are then consumed by the sink
        configMap.put("debezium.transforms", "flatten,question-to-header,answer-to-header,context-to-header");
        configMap.put("debezium.transforms.flatten.type", ExtractNewRecordState.class.getName());
        configMap.put("debezium.transforms.question-to-header.type", HeaderFrom.Value.class.getName());
        configMap.put("debezium.transforms.question-to-header.fields", "purchaser");
        configMap.put("debezium.transforms.question-to-header.headers", "question");
        configMap.put("debezium.transforms.question-to-header.operation", "copy");
        configMap.put("debezium.transforms.answer-to-header.type", HeaderFrom.Value.class.getName());
        configMap.put("debezium.transforms.answer-to-header.fields", "product_id");
        configMap.put("debezium.transforms.answer-to-header.headers", "answer");
        configMap.put("debezium.transforms.answer-to-header.operation", "copy");
        configMap.put("debezium.transforms.context-to-header.type", HeaderFrom.Value.class.getName());
        configMap.put("debezium.transforms.context-to-header.fields", "quantity");
        configMap.put("debezium.transforms.context-to-header.headers", "context");
        configMap.put("debezium.transforms.context-to-header.operation", "copy");

        configMap.put("quarkus.log.min-level", "TRACE");
        configMap.put("quarkus.log.category.\"io.debezium.server.instructlab\".level", "TRACE");

        config = configMap;
    }

    @Override
    public int getOrdinal() {
        // Configuration property precedence is based on ordinal values and since we override the
        // properties in TestConfigSource, we should give this a higher priority.
        return super.getOrdinal() + 1;
    }
}
