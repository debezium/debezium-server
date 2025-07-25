/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.eventhubs;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.function.Function;

import io.debezium.util.HexConverter;
import io.debezium.util.Strings;

/**
 * Enumeration of supported hash functions for message key hashing.
 */
public enum HashFunction {
    JAVA("java") {
        @Override
        public Function<String, String> hash() {
            return input -> String.valueOf(input.hashCode());
        }
    },
    MD5("md5") {
        @Override
        public Function<String, String> hash() {
            return new Function<String, String>() {
                private final MessageDigest digest = createDigest("MD5");

                @Override
                public String apply(String input) {
                    return computeDigest(input, digest);
                }
            };
        }
    },
    SHA1("sha1") {
        @Override
        public Function<String, String> hash() {
            return new Function<String, String>() {
                private final MessageDigest digest = createDigest("SHA-1");

                @Override
                public String apply(String input) {
                    return computeDigest(input, digest);
                }
            };
        }
    },
    SHA256("sha256") {
        @Override
        public Function<String, String> hash() {
            return new Function<String, String>() {
                private final MessageDigest digest = createDigest("SHA-256");

                @Override
                public String apply(String input) {
                    return computeDigest(input, digest);
                }
            };
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
     * Returns a function that computes the hash of the input string using this hash function.
     * Each call returns a new function instance with its own MessageDigest for thread safety.
     *
     * @return a function that takes a string input and returns the hashed string
     */
    public abstract Function<String, String> hash();

    /**
    * Parse a string value to a HashFunction enum.
    *
    * @param value the string value (case-insensitive)
    * @return the corresponding HashFunction, or null if value is null/blank
    * @throws IllegalArgumentException if the value is not supported
    */
    public static HashFunction fromString(String value) {
        if (Strings.isNullOrBlank(value)) {
            return null;
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
     * @param digest the MessageDigest to use (thread-safe as each function has its own instance)
     * @return the hex-encoded hash string
     */
    private static String computeDigest(String input, MessageDigest digest) {
        digest.reset();
        byte[] hashBytes = digest.digest(input.getBytes());
        return HexConverter.convertToHexString(hashBytes);
    }
}
