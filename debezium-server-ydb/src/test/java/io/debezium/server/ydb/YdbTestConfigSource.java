/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ydb;

import java.util.HashMap;
import java.util.Map;

import io.debezium.server.TestConfigSource;

public class YdbTestConfigSource extends TestConfigSource {

    public static final String CONSUMER = "debezium-it-consumer";

    public static final String TOPIC = "/local/debezium-it/default";

    public static final String TOPIC_PREFIX = "/local/cdc";

    public static final String DESTINATION_CUSTOMERS = "testc.inventory.customers";
    public static final String DESTINATION_PRODUCTS = "testc.inventory.products";

    public static final String JDBC_URL = "jdbc:ydb:grpc://localhost:2136/local";
    public static final String OFFSET_TABLE = "`debezium_offset_storage`";

    private static final String OFFSET_DDL = "CREATE TABLE `debezium_offset_storage` ("
            + "id Utf8 NOT NULL, offset_key Utf8, offset_val Utf8, "
            + "record_insert_ts Timestamp NOT NULL, record_insert_seq Int32 NOT NULL, "
            + "PRIMARY KEY(id))";

    private static final String OFFSET_SELECT = "SELECT id, offset_key, offset_val FROM `debezium_offset_storage` "
            + "ORDER BY record_insert_ts, record_insert_seq";

    private static final String OFFSET_DELETE = "DELETE FROM `debezium_offset_storage`";

    private static final String OFFSET_INSERT = "INSERT INTO `debezium_offset_storage`"
            + "(id, offset_key, offset_val, record_insert_ts, record_insert_seq) VALUES (?, ?, ?, ?, ?)";

    public YdbTestConfigSource() {
        super();
        if (!isItTest()) {
            return;
        }
        config = ydbIntegrationConfig();
    }

    private static Map<String, String> ydbIntegrationConfig() {
        Map<String, String> ydbTest = new HashMap<>();

        ydbTest.put("debezium.sink.type", "ydb");
        ydbTest.put("debezium.sink.ydb.endpoint", YdbTestResourceLifecycleManager.ENDPOINT);
        ydbTest.put("debezium.sink.ydb.database", YdbTestResourceLifecycleManager.DATABASE);
        ydbTest.put("debezium.sink.ydb.connector.name", "debezium-it");
        ydbTest.put("debezium.sink.ydb.instance.id", "it-instance");

        ydbTest.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        ydbTest.put("debezium.source.offset.flush.interval.ms", "0");
        ydbTest.put("debezium.source.topic.prefix", "testc");
        ydbTest.put("debezium.source.schema.include.list", "inventory");
        ydbTest.put("debezium.source.table.include.list", "inventory.customers");
        ydbTest.put("debezium.format.value", "json");
        ydbTest.put("debezium.format.key", "json");

        putJdbcOffsetStorage(ydbTest, JDBC_URL,
                YdbTestResourceLifecycleManager.JDBC_USER,
                YdbTestResourceLifecycleManager.JDBC_PASSWORD);

        return ydbTest;
    }

    static void putJdbcOffsetStorage(Map<String, String> cfg, String jdbcUrl, String user, String password) {
        cfg.put("debezium.source.offset.storage", "io.debezium.storage.jdbc.offset.JdbcOffsetBackingStore");
        cfg.put("debezium.source.offset.storage.jdbc.connection.url", jdbcUrl);
        cfg.put("debezium.source.offset.storage.jdbc.connection.user", user);
        cfg.put("debezium.source.offset.storage.jdbc.connection.password", password);
        cfg.put("debezium.source.offset.storage.jdbc.table.name", "debezium_offset_storage");
        cfg.put("debezium.source.offset.storage.jdbc.table.ddl", OFFSET_DDL);
        cfg.put("debezium.source.offset.storage.jdbc.table.select", OFFSET_SELECT);
        cfg.put("debezium.source.offset.storage.jdbc.table.delete", OFFSET_DELETE);
        cfg.put("debezium.source.offset.storage.jdbc.table.insert", OFFSET_INSERT);
    }

    public static String prefixedTopic(String destination) {
        return YdbChangeConsumer.resolveTopicPath(null, TOPIC_PREFIX, destination);
    }

    public static int waitForSeconds() {
        return TestUtils.waitTimeForRecords();
    }

    @Override
    public int getOrdinal() {
        return super.getOrdinal() + 1;
    }
}
