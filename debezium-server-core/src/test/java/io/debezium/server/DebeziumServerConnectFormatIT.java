/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

/**
 * Integration test that verifies Debezium server is able to deliver records in Kafka Connect format.
 *
 * @author vjuranek
 */
@QuarkusTest
@TestProfile(DebeziumServerConnectFormatProfile.class)
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
@EnabledIfSystemProperty(named = "test.apicurio", matches = "false", disabledReason = "DebeziumServerConfigProvidersIT doesn't run with apicurio profile.")
@DisabledIfSystemProperty(named = "debezium.format.key", matches = "protobuf")
@DisabledIfSystemProperty(named = "debezium.format.value", matches = "protobuf")
public class DebeziumServerConnectFormatIT {

    private static final int MESSAGE_COUNT = 4;
    @Inject
    DebeziumServer server;

    {
        Testing.Files.delete(TestConfigSource.OFFSET_STORE_PATH);
    }

    void setupDependencies(@Observes ConnectorStartedEvent event) {
        if (!TestConfigSource.isItTest()) {
            return;
        }

    }

    void connectorCompleted(@Observes ConnectorCompletedEvent event) throws Exception {
        if (!event.isSuccess()) {
            throw (Exception) event.getError().get();
        }
    }

    @Test
    public void testPostgresWithSourceRecord() throws Exception {
        Testing.Print.enable();
        final TestConsumer testConsumer = (TestConsumer) server.getConsumer();
        Awaitility.await().atMost(Duration.ofSeconds(TestConfigSource.waitForSeconds()))
                .until(() -> (testConsumer.getValues().size() >= MESSAGE_COUNT));
        assertThat(testConsumer.getValues().size()).isEqualTo(MESSAGE_COUNT);

        SourceRecord record = (SourceRecord) testConsumer.getValues().get(MESSAGE_COUNT - 1);
        Struct key = (Struct) record.key();
        assertThat(key.getInt32("id")).isEqualTo(1004);

        Struct after = ((Struct) record.value()).getStruct("after");
        assertThat(after.getInt32("id")).isEqualTo(1004);
        assertThat(after.getString("first_name")).isEqualTo("Anne");
        assertThat(after.getString("last_name")).isEqualTo("Kretchmar");
        assertThat(after.getString("email")).isEqualTo("annek@noanswer.org");

        ConnectHeaders headers = (ConnectHeaders) record.headers();
        assertThat(headers.size()).isEqualTo(0);
    }
}
