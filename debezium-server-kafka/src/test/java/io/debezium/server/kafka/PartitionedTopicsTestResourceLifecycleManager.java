/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.kafka;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

/**
 * Creates the sink topics with more than one partition before the connector starts.
 * The broker creates a topic with one partition on its own, so the test could not see the routing.
 */
public class PartitionedTopicsTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

    public static final List<String> TOPICS = List.of("testc.inventory.customers", "testc.inventory.products");

    @Override
    public Map<String, String> start() {
        final Map<String, Object> config = Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaTestResourceLifecycleManager.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(config)) {
            admin.createTopics(TOPICS.stream()
                    .map(topic -> new NewTopic(topic, PartitionRoutingProfile.PARTITION_COUNT, (short) 1))
                    .toList())
                    .all()
                    .get(60, TimeUnit.SECONDS);
        }
        catch (Exception e) {
            throw new IllegalStateException("Could not create the partitioned test topics", e);
        }
        return Map.of();
    }

    @Override
    public void stop() {
    }
}
