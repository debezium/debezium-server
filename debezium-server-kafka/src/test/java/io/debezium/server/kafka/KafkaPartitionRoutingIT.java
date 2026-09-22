/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

/**
 * Checks that the Kafka sink sends each record to the partition that the {@code PartitionRouting} SMT selected.
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
@QuarkusTestResource(KafkaTestResourceLifecycleManager.class)
@QuarkusTestResource(PartitionedTopicsTestResourceLifecycleManager.class)
@TestProfile(PartitionRoutingProfile.class)
public class KafkaPartitionRoutingIT extends KafkaBaseIT {

    private static final int CUSTOMER_COUNT = 4;
    private static final int PRODUCT_COUNT = 9;
    private static final Pattern KEY_ID = Pattern.compile("\"payload\":\\{\"id\":(\\d+)\\}");

    @Test
    @FixFor("debezium/dbz#2691")
    public void testRecordsLandOnThePartitionSelectedByTheSmt() {
        Awaitility.await().atMost(Duration.ofSeconds(KafkaTestConfigSource.waitForSeconds())).until(() -> consumer != null);
        consumer.subscribe(PartitionedTopicsTestResourceLifecycleManager.TOPICS);

        final List<ConsumerRecord<String, String>> actual = new ArrayList<>();
        Awaitility.await()
                .atMost(Duration.ofSeconds(KafkaTestConfigSource.waitForSeconds()))
                .until(() -> {
                    consumer.poll(Duration.ofSeconds(KafkaTestConfigSource.waitForSeconds()))
                            .iterator()
                            .forEachRemaining(actual::add);
                    return actual.size() >= CUSTOMER_COUNT + PRODUCT_COUNT;
                });

        for (ConsumerRecord<String, String> record : actual) {
            final int id = idOf(record.key());
            assertThat(record.partition())
                    .as("record %s with id %d", record.topic(), id)
                    .isEqualTo(id % PartitionRoutingProfile.PARTITION_COUNT);
        }
        assertThat(actual.stream().map(ConsumerRecord::partition).distinct()).hasSize(PartitionRoutingProfile.PARTITION_COUNT);
    }

    private static int idOf(String key) {
        final Matcher matcher = KEY_ID.matcher(key);
        assertThat(matcher.find()).as("key %s carries the row id", key).isTrue();
        return Integer.parseInt(matcher.group(1));
    }
}
