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
    TRUNCATE,
    MESSAGE,
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
            case "t" -> TRUNCATE;
            case "m" -> MESSAGE;
            default -> CHANGE;
        };
    }

    static ZerobusOperation fromFilterToken(String token) {
        return switch (token.toLowerCase(Locale.ROOT)) {
            case "c", "create" -> CREATE;
            case "r", "read" -> READ;
            case "u", "update" -> UPDATE;
            case "d", "delete" -> DELETE;
            case "t", "truncate" -> TRUNCATE;
            case "m", "message" -> MESSAGE;
            case "change" -> CHANGE;
            case "tombstone" -> TOMBSTONE;
            default -> throw new IllegalArgumentException("Unsupported operation token: " + token);
        };
    }
}
