/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ydb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

@QuarkusTest
@TestProfile(YdbDefaultTestProfile.class)
@Timeout(120)
public class YdbDefaultIT extends YdbITBase {

    private static final int TEST_ID = 9001;
    private static final int OUTAGE_TEST_ID = 9102;
    private static final int DEDUP_TEST_ID = 9201;

    @Test
    public void deliversStreamingUpdateAndDelete() throws Exception {
        try (TestUtils.TopicCapture capture = TestUtils.captureTopic(
                YdbTestConfigSource.TOPIC, YdbTestConfigSource.CONSUMER)) {

            try (PostgresConnection pg = TestUtils.getPostgresConnection()) {
                pg.execute(
                        "INSERT INTO inventory.customers (id, first_name, last_name, email) "
                                + "VALUES (" + TEST_ID + ", 'Mike', 'Levin', 'mike@example.com')");
            }
            YdbMessageAssertions.awaitEvent(capture::snapshot, "c", "customers", TEST_ID);

            try (PostgresConnection pg = TestUtils.getPostgresConnection()) {
                pg.execute("UPDATE inventory.customers SET first_name = 'Mikhail' WHERE id = " + TEST_ID);
            }
            YdbMessageAssertions.awaitEvent(capture::snapshot, "u", "customers", TEST_ID);

            try (PostgresConnection pg = TestUtils.getPostgresConnection()) {
                pg.execute("ALTER TABLE inventory.customers REPLICA IDENTITY FULL");
                pg.execute("DELETE FROM inventory.customers WHERE id = " + TEST_ID);
            }
            TestUtils.waitBoolean(() -> {
                try {
                    return YdbMessageAssertions.hasDeleteEvent(capture.snapshot(), "customers", TEST_ID);
                }
                catch (Exception e) {
                    return false;
                }
            });
        }

        try (TestUtils.YdbAdmin admin = TestUtils.openAdmin()) {
            assertThat(admin.topicExists(YdbTestConfigSource.TOPIC)).isTrue();
        }
    }

    @Test
    public void resumesStreamingAfterYdbOutage() throws Exception {
        try (TestUtils.TopicCapture capture = TestUtils.captureTopic(
                YdbTestConfigSource.TOPIC, YdbTestConfigSource.CONSUMER)) {

            YdbTestResourceLifecycleManager.pause();
            try (PostgresConnection pg = TestUtils.getPostgresConnection()) {
                pg.execute(
                        "INSERT INTO inventory.customers (id, first_name, last_name, email) "
                                + "VALUES (" + OUTAGE_TEST_ID + ", 'Outage', 'Test', 'outage@example.com')");
            }
            Thread.sleep(3_000);

            YdbTestResourceLifecycleManager.unpause();
            YdbTestResourceLifecycleManager.waitUntilReady();

            YdbMessageAssertions.awaitEvent(capture::snapshot, "c", "customers", OUTAGE_TEST_ID);
        }
    }

    @Test
    public void atLeastOnceDuplicatesCollapseBySourceLsn() throws Exception {
        try (TestUtils.TopicCapture capture = TestUtils.captureTopic(
                YdbTestConfigSource.TOPIC, YdbTestConfigSource.CONSUMER)) {

            try (PostgresConnection pg = TestUtils.getPostgresConnection()) {
                pg.execute(
                        "INSERT INTO inventory.customers (id, first_name, last_name, email) "
                                + "VALUES (" + DEDUP_TEST_ID + ", 'Dedup', 'Test', 'dedup@example.com')");
            }
            YdbMessageAssertions.awaitEvent(capture::snapshot, "c", "customers", DEDUP_TEST_ID);

            var creates = YdbMessageAssertions.createEventsForId(capture.snapshot(), "customers", DEDUP_TEST_ID);
            assertThat(creates).isNotEmpty();

            long distinct = YdbMessageAssertions.countDistinctEventKeys(creates);
            assertThat(YdbMessageAssertions.deduplicate(creates)).hasSize((int) distinct);

            if (creates.size() > distinct) {
                assertThat(distinct).isEqualTo(1);
            }
        }
    }

    @Test
    public void offsetsArePersistedToYdbViaJdbc() {
        assertOffsetsPersisted();
    }
}
