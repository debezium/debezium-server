/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis.wip;

public final class TestProperties {
    private TestProperties() {
        // intentionally private
    }

    public static final String DEBEZIUM_VERSION = System.getProperty("test.version.debezium");
    public static final String DEBEZIUM_SERVER_IMAGE_GROUP = System.getProperty("test.server.image.group");
}
