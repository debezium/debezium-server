/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.eventhubs;

/**
 * Enumeration of supported hash functions for message key hashing.
 */
public enum HashFunction {
    JAVA("java"),
    MD5("md5"),
    SHA1("sha1"),
    SHA256("sha256");

    private final String value;

    HashFunction(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    /**
     * Parse a string value to a HashFunction enum.
     * 
     * @param value the string value (case-insensitive)
     * @return the corresponding HashFunction
     * @throws IllegalArgumentException if the value is not supported
     */
    public static HashFunction fromString(String value) {
        if (value == null) {
            return JAVA; // default
        }

        String lowerValue = value.toLowerCase();
        for (HashFunction hashFunction : values()) {
            if (hashFunction.value.equals(lowerValue)) {
                return hashFunction;
            }
        }

        throw new IllegalArgumentException("Unsupported hash function: " + value + 
            ". Supported values are: java, md5, sha1, sha256");
    }
} 