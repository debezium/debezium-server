/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for the RFC 9396 {@code authorization_details} payload built from the configured
 * fully-qualified tables. Zerobus's OAuth grant requires precisely {@code USE CATALOG} / {@code USE
 * SCHEMA} on the parents and {@code SELECT} + {@code MODIFY} on each table (never
 * {@code ALL_PRIVILEGES}), so this encoding is security-relevant and worth pinning.
 */
class AuthorizationDetailsTest {

    @Test
    void singleTableProducesCatalogSchemaTablePrivileges() {
        String json = ZerobusOAuthBearerLoginCallbackHandler.buildAuthorizationDetails("main.default.customers");

        assertThat(json)
                .contains("\"privileges\":[\"USE CATALOG\"],\"object_type\":\"CATALOG\",\"object_full_path\":\"main\"")
                .contains("\"privileges\":[\"USE SCHEMA\"],\"object_type\":\"SCHEMA\",\"object_full_path\":\"main.default\"")
                .contains("\"privileges\":[\"SELECT\",\"MODIFY\"],\"object_type\":\"TABLE\",\"object_full_path\":\"main.default.customers\"");
        // Must never request ALL_PRIVILEGES (Zerobus rejects it: invalid_authorization_details).
        assertThat(json).doesNotContain("ALL_PRIVILEGES");
        assertThat(json).startsWith("[").endsWith("]");
    }

    @Test
    void deduplicatesSharedCatalogAndSchemaAcrossTables() {
        String json = ZerobusOAuthBearerLoginCallbackHandler.buildAuthorizationDetails(
                "main.default.customers, main.default.orders");

        // one CATALOG entry, one SCHEMA entry, two TABLE entries
        assertThat(countOccurrences(json, "\"object_type\":\"CATALOG\"")).isEqualTo(1);
        assertThat(countOccurrences(json, "\"object_type\":\"SCHEMA\"")).isEqualTo(1);
        assertThat(countOccurrences(json, "\"object_type\":\"TABLE\"")).isEqualTo(2);
        assertThat(json).contains("main.default.customers").contains("main.default.orders");
    }

    @Test
    void handlesMultipleCatalogsAndSchemas() {
        String json = ZerobusOAuthBearerLoginCallbackHandler.buildAuthorizationDetails(
                "c1.s1.t1, c2.s2.t2");

        assertThat(countOccurrences(json, "\"object_type\":\"CATALOG\"")).isEqualTo(2);
        assertThat(countOccurrences(json, "\"object_type\":\"SCHEMA\"")).isEqualTo(2);
        assertThat(countOccurrences(json, "\"object_type\":\"TABLE\"")).isEqualTo(2);
    }

    @Test
    void trimsWhitespaceAndSkipsEmptyEntries() {
        String json = ZerobusOAuthBearerLoginCallbackHandler.buildAuthorizationDetails(
                "  main.default.customers ,, ");

        assertThat(countOccurrences(json, "\"object_type\":\"TABLE\"")).isEqualTo(1);
        assertThat(json).contains("\"object_full_path\":\"main.default.customers\"");
    }

    @Test
    void rejectsNonQualifiedTable() {
        assertThatThrownBy(() -> ZerobusOAuthBearerLoginCallbackHandler.buildAuthorizationDetails("main.customers"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fully qualified");
    }

    private static int countOccurrences(String haystack, String needle) {
        int count = 0;
        int idx = 0;
        while ((idx = haystack.indexOf(needle, idx)) != -1) {
            count++;
            idx += needle.length();
        }
        return count;
    }
}
