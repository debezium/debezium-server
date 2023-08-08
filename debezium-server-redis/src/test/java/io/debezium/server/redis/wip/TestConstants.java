/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis.wip;

public class TestConstants {
    // POSTGRESQL
    public static final String POSTGRES_USER = "debezium";
    public static final String POSTGRES_PASSWORD = "dbz";
    public static final String POSTGRES_DATABASE = "debezium";
    public static final String POSTGRES_IMAGE = "quay.io/debezium/example-postgres";
    public static final int POSTGRES_PORT = 5432;

    // REDIS
    public static final String REDIS_IMAGE = "redis";
    public static final int REDIS_PORT = 6379;

}
