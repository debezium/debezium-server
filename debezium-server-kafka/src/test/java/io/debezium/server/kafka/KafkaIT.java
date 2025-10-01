/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.kafka;

import org.junit.jupiter.api.Test;

import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

/**
 * Integration test for verifying that the Kafka sink adapter can stream change events from a PostgreSQL database
 * to a configured Apache Kafka broker.
 *
 * @author Alfusainey Jallow
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
@QuarkusTestResource(KafkaTestResourceLifecycleManager.class)
public class KafkaIT extends KafkaBaseIT {

    @Test
    public void testKafka() {
        super.testKafka();
    }
}
