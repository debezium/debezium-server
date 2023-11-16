/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis.wip;

import static io.debezium.server.redis.wip.TestConstants.MYSQL_DATABASE;
import static io.debezium.server.redis.wip.TestConstants.MYSQL_PASSWORD;
import static io.debezium.server.redis.wip.TestConstants.MYSQL_PORT;
import static io.debezium.server.redis.wip.TestConstants.MYSQL_USER;
import static io.debezium.server.redis.wip.TestConstants.REDIS_PORT;
import static io.debezium.server.redis.wip.TestConstants.REDIS_SSL_PORT;

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

    public static Map<String, String> baseRedisConfig(DebeziumTestContainerWrapper redis) {
        return Map.of(
                "debezium.sink.type", "redis",
                "debezium.sink.redis.address", redis.getContainerAddress() + ":" + REDIS_PORT);
    }

    public static Map<String, String> baseSslRedisConfig(DebeziumTestContainerWrapper redis) {
        return Map.of(
                "debezium.sink.type", "redis",
                "debezium.sink.redis.address", redis.getContainerAddress() + ":" + REDIS_SSL_PORT,
                "debezium.sink.redis.ssl.enabled", "true",
                "debezium.source.database.ssl.mode", "disabled",
                "debezium.source.offset.storage", "io.debezium.server.redis.RedisOffsetBackingStore");
    }

    public Map<String, String> baseMySqlConfig(DebeziumTestContainerWrapper mysql) {
        Map<String, String> result = new HashMap<>(Map.of("debezium.source.connector.class", "io.debezium.connector.mysql.MySqlConnector",
                "debezium.source.offset.flush.interval.ms", "0",
                "debezium.source.topic.prefix", "testc",
                "debezium.source.database.dbname", MYSQL_DATABASE,
                "debezium.source.database.hostname", String.valueOf(mysql.getContainerAddress()),
                "debezium.source.database.port", String.valueOf(MYSQL_PORT),
                "debezium.source.database.user", MYSQL_USER,
                "debezium.source.database.password", MYSQL_PASSWORD,
                "debezium.source.database.server.id", "1",
                "debezium.source.schema.history.internal", "io.debezium.server.redis.RedisSchemaHistory"));
        result.put("debezium.source.offset.storage.file.filename", "offset.dat");
        return result;
    }

    public DebeziumServerConfigBuilder withBaseMySqlRedisConfig(DebeziumTestContainerWrapper redis, DebeziumTestContainerWrapper mysql) {
        config.putAll(baseRedisConfig(redis));
        config.putAll(baseMySqlConfig(mysql));
        return this;
    }

    public DebeziumServerConfigBuilder withBaseMysqlSslRedisConfig(DebeziumTestContainerWrapper redis, DebeziumTestContainerWrapper mysql) {
        config.putAll(baseSslRedisConfig(redis));
        config.putAll(baseMySqlConfig(mysql));
        return this;
    }
}
