/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.sns;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;

/**
 * Unit tests for {@link SnsChangeConsumerConfig}, covering the neutral {@code message.group.id.*}
 * properties and their deprecated {@code fifo.*} aliases.
 */
public class SnsChangeConsumerConfigTest {

    @Test
    public void testMessageGroupIdHeaderResolvedFromNewProperty() {
        SnsChangeConsumerConfig config = new SnsChangeConsumerConfig(
                Configuration.from(Map.of("message.group.id.header", "tenantId")));
        assertEquals("tenantId", config.getMessageGroupIdHeader());
    }

    @Test
    public void testMessageGroupIdHeaderResolvedFromDeprecatedAlias() {
        SnsChangeConsumerConfig config = new SnsChangeConsumerConfig(
                Configuration.from(Map.of("fifo.message.group.id.header", "legacyHeader")));
        assertEquals("legacyHeader", config.getMessageGroupIdHeader());
    }

    @Test
    public void testMessageGroupIdHeaderNewPropertyWinsOverDeprecatedAlias() {
        SnsChangeConsumerConfig config = new SnsChangeConsumerConfig(Configuration.from(Map.of(
                "message.group.id.header", "newHeader",
                "fifo.message.group.id.header", "legacyHeader")));
        assertEquals("newHeader", config.getMessageGroupIdHeader());
    }

    @Test
    public void testDefaultMessageGroupIdResolvedFromNewProperty() {
        SnsChangeConsumerConfig config = new SnsChangeConsumerConfig(
                Configuration.from(Map.of("message.group.id.default", "shared-tenant")));
        assertEquals("shared-tenant", config.getDefaultMessageGroupId());
    }

    @Test
    public void testDefaultMessageGroupIdResolvedFromDeprecatedAlias() {
        SnsChangeConsumerConfig config = new SnsChangeConsumerConfig(
                Configuration.from(Map.of("fifo.default.group.id", "legacy-group")));
        assertEquals("legacy-group", config.getDefaultMessageGroupId());
    }

    @Test
    public void testDefaultMessageGroupIdNewPropertyWinsOverDeprecatedAlias() {
        SnsChangeConsumerConfig config = new SnsChangeConsumerConfig(Configuration.from(Map.of(
                "message.group.id.default", "new-group",
                "fifo.default.group.id", "legacy-group")));
        assertEquals("new-group", config.getDefaultMessageGroupId());
    }

    @Test
    public void testMessageGroupIdDisabledByDefault() {
        SnsChangeConsumerConfig config = new SnsChangeConsumerConfig(
                Configuration.from(Map.<String, String> of()));
        assertFalse(config.isMessageGroupIdEnabled());
    }

    @Test
    public void testGroupIdPropertiesFallBackToDefaults() {
        SnsChangeConsumerConfig config = new SnsChangeConsumerConfig(
                Configuration.from(Map.<String, String> of()));
        assertEquals("aggregateId", config.getMessageGroupIdHeader());
        assertEquals("default", config.getDefaultMessageGroupId());
    }
}
