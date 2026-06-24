/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.zerobus;

import java.util.Map;

public record ZeroBusRecord(
        String targetTable,
        String destination,
        Integer partition,
        Object key,
        Object value,
        Map<String, String> sourcePosition,
        Map<String, String> headers,
        ZeroBusOperation operation,
        String idempotencyKey) {
}
