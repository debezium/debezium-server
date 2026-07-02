/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import java.util.HashMap;
import java.util.Map;

import io.debezium.server.TestConfigSource;

public class RedisConfigSource extends TestConfigSource {

    public RedisConfigSource() {
        Map<String, String> redisTest = new HashMap<>();

        redisTest.put("debezium.sink.type", "redis");
        redisTest.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");

        config = redisTest;
    }

    @Override
    public int getOrdinal() {
        // Configuration property precedence is based on ordinal values and since we override the
        // properties in TestConfigSource, we should give this a higher priority.
        return super.getOrdinal() + 1;
    }
}
