/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.zerobus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;

class ZeroBusTableRouterTest {

    @Test
    void mapsSourceDestinationToDefaultUnityCatalogTable() {
        ZeroBusTableRouter router = new ZeroBusTableRouter(ZeroBusSinkConfigTest.baseConfig());

        assertThat(router.route(record("server.inventory.customers")))
                .isEqualTo("main.bronze.customers");
    }

    @Test
    void mapsExplicitDestinationOverride() {
        ZeroBusSinkConfig config = new ZeroBusSinkConfig(ZeroBusSinkConfigTest.baseBuilder()
                .with("table.mapping.mode", "explicit")
                .with("table.mapping.overrides", "server.inventory.customers=main.silver.customer_dim")
                .build());
        config.validate();

        assertThat(new ZeroBusTableRouter(config).route(record("server.inventory.customers")))
                .isEqualTo("main.silver.customer_dim");
    }

    @Test
    void mapsRegexDestinationWithoutKafkaConnectSmt() {
        ZeroBusSinkConfig config = new ZeroBusSinkConfig(ZeroBusSinkConfigTest.baseBuilder()
                .with("table.mapping.mode", "regex")
                .with("table.mapping.regex", "server\\.inventory\\.(.*)")
                .with("table.mapping.replacement", "main.bronze.$1")
                .build());
        config.validate();

        assertThat(new ZeroBusTableRouter(config).route(record("server.inventory.orders")))
                .isEqualTo("main.bronze.orders");
    }

    @Test
    void rejectsInvalidUnityCatalogTarget() {
        ZeroBusSinkConfig config = new ZeroBusSinkConfig(ZeroBusSinkConfigTest.baseBuilder()
                .with("table.mapping.mode", "explicit")
                .with("table.mapping.overrides", "server.inventory.customers=main.bad-schema.customer_dim")
                .build());

        config.validate();

        assertThat(new ZeroBusTableRouter(config).route(record("server.inventory.customers")))
                .isEqualTo("main.bad-schema.customer_dim");
    }

    @Test
    void rejectsUnityCatalogNamesWithDisallowedCharacters() {
        ZeroBusSinkConfig config = new ZeroBusSinkConfig(ZeroBusSinkConfigTest.baseBuilder()
                .with("table.mapping.mode", "explicit")
                .with("table.mapping.overrides", "server.inventory.customers=main.bad/schema.customer_dim")
                .build());

        assertThatThrownBy(config::validate)
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("Invalid ZeroBus Unity Catalog identifier");
    }

    @Test
    void rejectsInvalidRegexReplacementDuringConfigValidation() {
        ZeroBusSinkConfig config = new ZeroBusSinkConfig(ZeroBusSinkConfigTest.baseBuilder()
                .with("table.mapping.mode", "regex")
                .with("table.mapping.regex", "server\\.inventory\\.(.*)")
                .with("table.mapping.replacement", "main.bronze.$2")
                .build());

        assertThatThrownBy(config::validate)
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("table.mapping.replacement");
    }

    @SuppressWarnings("unchecked")
    static ChangeEvent<Object, Object> record(String destination) {
        ChangeEvent<Object, Object> record = mock(ChangeEvent.class);
        when(record.destination()).thenReturn(destination);
        when(record.key()).thenReturn("1");
        when(record.value()).thenReturn("{\"after\":{\"id\":1}}");
        when(record.headers()).thenReturn(List.of());
        return record;
    }
}
