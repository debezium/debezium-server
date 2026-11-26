/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import java.io.ObjectInputStream;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

import org.eclipse.microprofile.config.ConfigProvider;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.AbstractSourceInfo;

/**
 * Helpers shared by the shutdown-after-snapshot tests.
 */
final class ShutdownAfterSnapshotSupport {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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
     * The file offset store keeps a Java-serialized map of partition and offset bytes; each offset is a JSON object.
     * <p>
     * While a snapshot runs, the connector stores the {@code snapshot} key in its offsets ({@code "INITIAL"} in PostgreSQL,
     * {@code true} in other connectors). The last record of the batch is the notification that ends the snapshot, and its
     * offset is taken once the snapshot has completed, without that key. A skipped snapshot leaves such an offset too, so the
     * tests start from fresh offsets and also check that the whole snapshot was delivered.
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
            if (offsets.isEmpty()) {
                return false;
            }
            for (byte[] value : offsets.values()) {
                if (OBJECT_MAPPER.readTree(value).has(AbstractSourceInfo.SNAPSHOT_KEY)) {
                    return false;
                }
            }
            return true;
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
