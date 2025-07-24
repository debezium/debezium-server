/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.eventhubs;

import io.debezium.util.HexConverter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Enumeration of supported hash functions for message key hashing.
 */
public enum HashFunction {
    JAVA("java") {
        @Override
        public String hash(String input) {
            return String.valueOf(input.hashCode());
        }
    },
    MD5("md5") {
        private final ThreadLocal<MessageDigest> digest = ThreadLocal.withInitial(() -> createDigest("MD5"));

        @Override
        public String hash(String input) {
            return computeDigest(input, digest.get());
        }
    },
    SHA1("sha1") {
        private final ThreadLocal<MessageDigest> digest = ThreadLocal.withInitial(() -> createDigest("SHA-1"));

        @Override
        public String hash(String input) {
            return computeDigest(input, digest.get());
        }
    },
    SHA256("sha256") {
        private final ThreadLocal<MessageDigest> digest = ThreadLocal.withInitial(() -> createDigest("SHA-256"));

        @Override
        public String hash(String input) {
            return computeDigest(input, digest.get());
        }
    };

    private final String value;

    HashFunction(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    /**
     * Computes the hash of the input string using this hash function.
     *
     * @param input the string to hash
     * @return the hashed string
     */
    public abstract String hash(String input);

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

    /**
     * Creates a MessageDigest instance for the specified algorithm.
     *
     * @param algorithm the digest algorithm
     * @return the MessageDigest instance
     */
    private static MessageDigest createDigest(String algorithm) {
        try {
            return MessageDigest.getInstance(algorithm);
        }
        catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Hash algorithm not available: " + algorithm, e);
        }
    }

    /**
     * Computes a message digest hash for the input string.
     *
     * @param input the string to hash
     * @param digest the MessageDigest to use (thread-safe via ThreadLocal)
     * @return the hex-encoded hash string
     */
    private static String computeDigest(String input, MessageDigest digest) {
        digest.reset();
        byte[] hashBytes = digest.digest(input.getBytes());
        return HexConverter.convertToHexString(hashBytes);
    }
}