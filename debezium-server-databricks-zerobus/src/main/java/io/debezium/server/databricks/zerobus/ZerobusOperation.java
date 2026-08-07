/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import java.util.Locale;

enum ZerobusOperation {
    CREATE,
    READ,
    UPDATE,
    CHANGE,
    DELETE,
    TOMBSTONE;

    static ZerobusOperation fromDebeziumCode(String code) {
        if (code == null || code.isBlank()) {
            return CHANGE;
        }
        return switch (code.toLowerCase(Locale.ROOT)) {
            case "c" -> CREATE;
            case "r" -> READ;
            case "u" -> UPDATE;
            case "d" -> DELETE;
            default -> CHANGE;
        };
    }
}
