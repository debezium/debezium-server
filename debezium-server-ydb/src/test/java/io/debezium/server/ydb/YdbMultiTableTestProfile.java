/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ydb;

import java.util.Map;

public class YdbMultiTableTestProfile extends YdbBaseTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of(
                "debezium.sink.ydb.topic.prefix", YdbTestConfigSource.TOPIC_PREFIX,
                "debezium.source.table.include.list", "inventory.customers,inventory.products");
    }
}
