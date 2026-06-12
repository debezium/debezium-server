/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import java.util.HashMap;
import java.util.Map;

import io.quarkus.test.junit.QuarkusTestProfile;

public class DebeziumServerConfigProfile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
        Map<String, String> config = new HashMap<>();

        config.put("debezium.transforms", "hoist,addheader");
        config.put("debezium.transforms.hoist.type", "org.apache.kafka.connect.transforms.HoistField$Value");
        config.put("debezium.transforms.hoist.field", "line");
        config.put("debezium.transforms.hoist.predicate", "topicNameMatch");
        config.put("debezium.transforms.addheader.type", "org.apache.kafka.connect.transforms.InsertHeader");
        config.put("debezium.transforms.addheader.header", "headerKey");
        config.put("debezium.transforms.addheader.value.literal", "headerValue");

        config.put("debezium.predicates", "topicNameMatch");
        config.put("debezium.predicates.topicNameMatch.type", "org.apache.kafka.connect.transforms.predicates.TopicNameMatches");
        config.put("debezium.predicates.topicNameMatch.pattern", ".*");

        config.put("DEBEZIUM_SOURCE_OFFSET_FLUSH_INTERVAL_MS_TEST", "0");
        config.put("debezium.source.snapshot.select.statement.overrides.public.table_name", "SELECT * FROM table_name WHERE 1>2");
        config.put("debezium.source.database.allowPublicKeyRetrieval", "true");

        return config;
    }
}
