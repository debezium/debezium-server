/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ydb;

import java.util.Map;

public class YdbDefaultTestProfile extends YdbBaseTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of(
                "debezium.sink.ydb.topic", YdbTestConfigSource.TOPIC,
                "debezium.sink.ydb.topic.auto.create", "true",
                "debezium.sink.ydb.topic.auto.create.initial.consumer", YdbTestConfigSource.CONSUMER,
                "debezium.sink.ydb.producer.codec", "gzip");
    }
}
