/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ydb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * JSON envelope checks for YDB topic ITs. {@link #eventKey(JsonNode)} matches the logical change
 * (Postgres {@code source.lsn} + {@code op}) so at-least-once duplicates can be collapsed.
 */
public final class YdbMessageAssertions {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private YdbMessageAssertions() {
    }

    public static JsonNode payload(byte[] dataBytes) throws Exception {
        JsonNode envelope = MAPPER.readTree(dataBytes);
        return envelope.has("payload") ? envelope.get("payload") : envelope;
    }

    /**
     * Stable id for a single Debezium change event (WAL position + operation).
     */
    public static String eventKey(JsonNode pl) {
        JsonNode source = pl.path("source");
        String lsn = source.path("lsn").asText(null);
        assertThat(lsn).as("source.lsn required for deduplication").isNotBlank();
        return source.path("db").asText() + "|"
                + source.path("schema").asText() + "|"
                + source.path("table").asText() + "|"
                + lsn + "|"
                + pl.path("op").asText();
    }

    public static String eventKey(byte[] dataBytes) throws Exception {
        return eventKey(payload(dataBytes));
    }

    public static List<TestUtils.CapturedMessage> deduplicate(Collection<TestUtils.CapturedMessage> messages) throws Exception {
        Map<String, TestUtils.CapturedMessage> unique = new LinkedHashMap<>();
        for (TestUtils.CapturedMessage message : messages) {
            unique.putIfAbsent(eventKey(message.data()), message);
        }
        return new ArrayList<>(unique.values());
    }

    public static long countDistinctEventKeys(Collection<TestUtils.CapturedMessage> messages) throws Exception {
        return messages.stream().map(m -> {
            try {
                return eventKey(m.data());
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).distinct().count();
    }

    public static List<TestUtils.CapturedMessage> createEventsForId(Collection<TestUtils.CapturedMessage> messages,
                                                                    String table, int id)
            throws Exception {
        List<TestUtils.CapturedMessage> matched = new ArrayList<>();
        for (TestUtils.CapturedMessage m : messages) {
            JsonNode pl = payload(m.data());
            if ("c".equals(pl.path("op").asText())
                    && table.equals(pl.path("source").path("table").asText())
                    && pl.path("after").path("id").asInt() == id) {
                matched.add(m);
            }
        }
        return matched;
    }

    public static boolean hasEvent(Collection<TestUtils.CapturedMessage> messages, String op, String table, int id) throws Exception {
        for (TestUtils.CapturedMessage m : messages) {
            JsonNode pl = payload(m.data());
            if (op.equals(pl.path("op").asText())
                    && table.equals(pl.path("source").path("table").asText())
                    && pl.path("after").path("id").asInt() == id) {
                return true;
            }
        }
        return false;
    }

    public static boolean hasDeleteEvent(Collection<TestUtils.CapturedMessage> messages, String table, int id) throws Exception {
        for (TestUtils.CapturedMessage m : messages) {
            JsonNode pl = payload(m.data());
            if ("d".equals(pl.path("op").asText())
                    && table.equals(pl.path("source").path("table").asText())
                    && pl.path("before").path("id").asInt() == id) {
                return true;
            }
        }
        return false;
    }

    public static void assertSnapshotRows(List<TestUtils.CapturedMessage> messages, String table, int minRows) throws Exception {
        assertThat(messages).hasSizeGreaterThanOrEqualTo(minRows);
        TestUtils.CapturedMessage first = messages.get(0);
        assertThat(first.key()).isNotNull();
        JsonNode pl = payload(first.data());
        assertThat(pl.path("source").path("table").asText()).isEqualTo(table);
        assertThat(pl.path("op").asText()).isIn("r", "c");
        JsonNode key = MAPPER.readTree(first.key());
        JsonNode keyPayload = key.has("payload") ? key.get("payload") : key;
        assertThat(keyPayload.has("id")).isTrue();
        assertThat(pl.path("source").path("lsn").isMissingNode()).isFalse();
    }

    public static void awaitEvent(Supplier<Collection<TestUtils.CapturedMessage>> snapshot,
                                  String op, String table, int id) {
        TestUtils.waitBoolean(() -> {
            try {
                return hasEvent(snapshot.get(), op, table, id);
            }
            catch (Exception e) {
                return false;
            }
        });
    }
}
