/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.jdbc;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

/**
 * Integration tests for JDBC sink with MySQL source and target.
 * <p>
 * Tests cover:
 * - Initial snapshot replication
 * - INSERT operations
 * - UPDATE operations (upsert mode)
 * - DELETE operations
 * - Schema evolution
 */
@QuarkusTest
@TestProfile(JdbcMySqlTestProfile.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class JdbcChangeConsumerMySqlIT extends JdbcChangeConsumerIT {

    @Override
    protected JdbcTestUtils testUtils() {
        return new JdbcMySqlTestUtils();
    }
}
