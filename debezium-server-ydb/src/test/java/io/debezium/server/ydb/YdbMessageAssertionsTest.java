/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ydb;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.Test;

public class YdbMessageAssertionsTest {

    private static final String EVENT_JSON = """
            {
              "op": "c",
              "source": {"db": "db", "schema": "inventory", "table": "customers", "lsn": 42},
              "after": {"id": 1}
            }
            """;

    @Test
    public void deduplicateCollapsesSameSourceLsn() throws Exception {
        byte[] data = EVENT_JSON.getBytes(StandardCharsets.UTF_8);
        TestUtils.CapturedMessage first = new TestUtils.CapturedMessage(data, null);
        TestUtils.CapturedMessage duplicate = new TestUtils.CapturedMessage(data.clone(), null);

        List<TestUtils.CapturedMessage> raw = List.of(first, duplicate);
        assertThat(raw).hasSize(2);
        assertThat(YdbMessageAssertions.deduplicate(raw)).hasSize(1);
        assertThat(YdbMessageAssertions.countDistinctEventKeys(raw)).isEqualTo(1);
    }

    @Test
    public void deduplicateKeepsDistinctLsn() throws Exception {
        byte[] first = EVENT_JSON.getBytes(StandardCharsets.UTF_8);
        byte[] second = EVENT_JSON.replace("42", "43").getBytes(StandardCharsets.UTF_8);

        List<TestUtils.CapturedMessage> raw = List.of(
                new TestUtils.CapturedMessage(first, null),
                new TestUtils.CapturedMessage(second, null));

        assertThat(YdbMessageAssertions.deduplicate(raw)).hasSize(2);
    }

    @Test
    public void deduplicateIsIdempotent() throws Exception {
        byte[] data = EVENT_JSON.getBytes(StandardCharsets.UTF_8);
        List<TestUtils.CapturedMessage> raw = List.of(
                new TestUtils.CapturedMessage(data, null),
                new TestUtils.CapturedMessage(data.clone(), null));

        List<TestUtils.CapturedMessage> once = YdbMessageAssertions.deduplicate(raw);
        List<TestUtils.CapturedMessage> twice = YdbMessageAssertions.deduplicate(once);
        assertThat(twice).containsExactlyElementsOf(once);
    }
}
