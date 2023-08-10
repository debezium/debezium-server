/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis.wip;

import static io.debezium.server.redis.wip.TestConstants.POSTGRES_DATABASE;
import static io.debezium.server.redis.wip.TestConstants.POSTGRES_PASSWORD;
import static io.debezium.server.redis.wip.TestConstants.POSTGRES_PORT;
import static io.debezium.server.redis.wip.TestConstants.POSTGRES_USER;
import static io.debezium.server.redis.wip.TestConstants.REDIS_PORT;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DebeziumServerConfigBuilder {
    private final Map<String, String> config = new HashMap<>();

    public DebeziumServerConfigBuilder withValue(String key, String value) {
        config.put(key, value);
        return this;
    }

    public List<String> build() {
        return config
                .entrySet()
                .stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.toList());
    }

    public Map<String, String> baseRedisConfig(DebeziumTestContainerWrapper redis) {
        return Map.of(
                "debezium.sink.type", "redis",
                "debezium.sink.redis.address", redis.getContainerIp() + ":" + REDIS_PORT);
    }

    public Map<String, String> basePostgresConfig(DebeziumTestContainerWrapper postgres) {
        return Map.of("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector",
                "debezium.source.offset.flush.interval.ms", "0",
                "debezium.source.topic.prefix", "testc",
                "debezium.source.schema.include.list", "inventory",
                "debezium.source.database.hostname", String.valueOf(postgres.getContainerIp()),
                "debezium.source.database.port", String.valueOf(POSTGRES_PORT),
                "debezium.source.database.user", POSTGRES_USER,
                "debezium.source.database.password", POSTGRES_PASSWORD,
                "debezium.source.database.dbname", POSTGRES_DATABASE,
                "debezium.source.offset.storage.file.filename", "offset.dat");
    }

    public DebeziumServerConfigBuilder withBaseConfig(DebeziumTestContainerWrapper redis, DebeziumTestContainerWrapper postgres) {
        config.putAll(baseRedisConfig(redis));
        config.putAll(basePostgresConfig(postgres));
        return this;
    }
}
