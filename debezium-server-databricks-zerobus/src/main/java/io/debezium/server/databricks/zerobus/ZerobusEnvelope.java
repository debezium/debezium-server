/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import java.util.Map;

record ZerobusEnvelope(
        String targetTable,
        String destination,
        Integer partition,
        Object key,
        Object value,
        Map<String, String> sourcePosition,
        Map<String, String> headers,
        ZerobusOperation operation,
        String idempotencyKey,
        Long sourceTimestampMillis) {
}
