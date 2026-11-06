/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import java.io.ObjectInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Helpers shared by the shutdown-after-snapshot tests.
 */
final class ShutdownAfterSnapshotSupport {

    private ShutdownAfterSnapshotSupport() {
    }

    /**
     * @return the number of rows in the table captured by the test source, read straight from the test database
     */
    static long expectedRows() throws Exception {
        final String host = ConfigProvider.getConfig().getValue("debezium.source.database.hostname", String.class);
        final String port = ConfigProvider.getConfig().getValue("debezium.source.database.port", String.class);
        final String dbName = ConfigProvider.getConfig().getValue("debezium.source.database.dbname", String.class);
        final String user = ConfigProvider.getConfig().getValue("debezium.source.database.user", String.class);
        final String password = ConfigProvider.getConfig().getValue("debezium.source.database.password", String.class);
        try (Connection connection = DriverManager.getConnection("jdbc:postgresql://" + host + ":" + port + "/" + dbName, user, password);
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SELECT count(*) FROM inventory.customers")) {
            rs.next();
            return rs.getLong(1);
        }
    }

    /**
     * The file offset store keeps a Java-serialized map of partition and offset bytes.
     *
     * @return {@code true} if an offset is stored and none of the stored offsets belongs to a snapshot that is still in progress
     */
    @SuppressWarnings("unchecked")
    static boolean completedSnapshotInOffsets() throws Exception {
        if (!Files.exists(TestConfigSource.OFFSET_STORE_PATH)) {
            return false;
        }
        try (ObjectInputStream in = new ObjectInputStream(Files.newInputStream(TestConfigSource.OFFSET_STORE_PATH))) {
            final Map<byte[], byte[]> offsets = (Map<byte[], byte[]>) in.readObject();
            return !offsets.isEmpty() && offsets.values().stream()
                    .map(value -> new String(value, StandardCharsets.UTF_8))
                    .noneMatch(json -> json.contains("\"snapshot\":true"));
        }
    }

    /**
     * @param value a value delivered to the test consumer
     * @return {@code true} if the value is a notification sent through the {@code sink} channel
     */
    static boolean isNotification(Object value) {
        return value != null && value.toString().contains("\"aggregate_type\"");
    }
}
