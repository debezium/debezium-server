/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.fluss;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

/**
 * Integration test that verifies the Fluss sink writes correctly to a primary-key table
 * when {@code primary.key.mode=upsert} is configured.
 *
 * @author Chris Cranford
 */
@QuarkusTest
@TestProfile(FlussUpsertModeProfile.class)
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
@QuarkusTestResource(FlussTestResourceLifecycleManager.class)
public class FlussUpsertModeIT {

    static final String TABLE_NAME = "testcu_inventory_customers";
    private static final int EXPECTED_RECORD_COUNT = 4;

    @BeforeEach
    public void beforeEach() {
        Testing.Files.delete(FlussTestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(FlussTestConfigSource.OFFSET_STORE_PATH);
    }

    @Test
    public void testFlussUpsertMode() {
        Testing.Print.enable();

        final String bootstrapServers = FlussTestResourceLifecycleManager.getBootstrapServers();
        final TablePath tablePath = TablePath.of(FlussTestConfigSource.DEFAULT_DATABASE, TABLE_NAME);

        final Configuration flussConfig = new Configuration();
        flussConfig.setString(FlussChangeConsumerConfig.BOOTSTRAP_SERVERS.name(), bootstrapServers);

        final List<Map<String, Object>> records = new ArrayList<>();
        Awaitility.await()
                .atMost(Duration.ofSeconds(FlussTestConfigSource.waitForSeconds()))
                .until(() -> {
                    try (Connection conn = ConnectionFactory.createConnection(flussConfig)) {
                        Table table = conn.getTable(tablePath);
                        try (LogScanner scanner = table.newScan().createLogScanner()) {
                            scanner.subscribeFromBeginning(0);
                            ScanRecords scanRecords = scanner.poll(Duration.ofSeconds(5));
                            for (ScanRecord record : scanRecords) {
                                records.add(Map.of("offset", record.logOffset()));
                            }
                        }
                    }
                    catch (Exception e) {
                        // table may not be ready yet
                    }
                    return records.size() >= EXPECTED_RECORD_COUNT;
                });

        assertThat(records.size()).isGreaterThanOrEqualTo(EXPECTED_RECORD_COUNT);
    }
}