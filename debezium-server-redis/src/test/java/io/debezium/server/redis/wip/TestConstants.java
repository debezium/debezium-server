/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis.wip;

public class TestConstants {
    private TestConstants() {
        // intentionally private
    }

    public static final int INITIAL_CUSTOMER_COUNT = 4;
    public static final int INITIAL_SCHEMA_HISTORY_SIZE = 16;
    public static final String LOCALHOST = "localhost";

    // REDIS
    public static final String REDIS_IMAGE = "redis";
    public static final int REDIS_PORT = 6379;

    // MYSQL
    public static final String MYSQL_USER = "debezium";
    public static final String MYSQL_PASSWORD = "dbz";
    public static final String MYSQL_ROOT_PASSWORD = "debezium";
    public static final String MYSQL_DATABASE = "inventory";
    public static final String MYSQL_PRIVILEGED_USER = "mysqluser";
    public static final String MYSQL_PRIVILEGED_PASSWORD = "mysqlpassword";
    public static final int MYSQL_PORT = 3306;
}
