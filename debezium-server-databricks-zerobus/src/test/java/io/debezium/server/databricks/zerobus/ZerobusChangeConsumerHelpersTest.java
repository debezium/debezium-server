/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for the static routing/skip helpers used by {@link ZerobusChangeConsumer}. These need
 * no Zerobus SDK, connection or native library, so they run in any CI environment.
 */
class ZerobusChangeConsumerHelpersTest {

    // --- isQualifiedTable: only catalog.schema.table (3 non-blank parts) is a valid Zerobus target ---

    @Test
    void qualifiedTableAcceptsThreePartName() {
        assertThat(ZerobusChangeConsumer.isQualifiedTable("main.default.customers")).isTrue();
    }

    @Test
    void qualifiedTableRejectsNull() {
        assertThat(ZerobusChangeConsumer.isQualifiedTable(null)).isFalse();
    }

    @Test
    void qualifiedTableRejectsTwoParts() {
        // e.g. a MySQL schema-history / DDL event emitted on the topic prefix
        assertThat(ZerobusChangeConsumer.isQualifiedTable("mysqldbz.customers")).isFalse();
    }

    @Test
    void qualifiedTableRejectsOnePart() {
        assertThat(ZerobusChangeConsumer.isQualifiedTable("mysqldbz")).isFalse();
    }

    @Test
    void qualifiedTableRejectsFourParts() {
        assertThat(ZerobusChangeConsumer.isQualifiedTable("a.b.c.d")).isFalse();
    }

    @Test
    void qualifiedTableRejectsBlankPart() {
        assertThat(ZerobusChangeConsumer.isQualifiedTable("main..customers")).isFalse();
        assertThat(ZerobusChangeConsumer.isQualifiedTable(".default.customers")).isFalse();
        assertThat(ZerobusChangeConsumer.isQualifiedTable("main.default.")).isFalse();
    }

    // --- isJsonObject: Zerobus only accepts a JSON object; tombstones / null payloads are skipped ---

    @Test
    void jsonObjectAcceptsObject() {
        assertThat(ZerobusChangeConsumer.isJsonObject("{\"id\":1}")).isTrue();
    }

    @Test
    void jsonObjectAcceptsObjectWithSurroundingWhitespace() {
        assertThat(ZerobusChangeConsumer.isJsonObject("  {\"id\":1}\n")).isTrue();
    }

    @Test
    void jsonObjectRejectsNull() {
        assertThat(ZerobusChangeConsumer.isJsonObject(null)).isFalse();
    }

    @Test
    void jsonObjectRejectsLiteralNullTombstone() {
        assertThat(ZerobusChangeConsumer.isJsonObject("null")).isFalse();
    }

    @Test
    void jsonObjectRejectsArray() {
        assertThat(ZerobusChangeConsumer.isJsonObject("[1,2,3]")).isFalse();
    }

    @Test
    void jsonObjectRejectsScalar() {
        assertThat(ZerobusChangeConsumer.isJsonObject("\"str\"")).isFalse();
    }
}
