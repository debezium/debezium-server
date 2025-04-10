/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.milvus;

import java.util.Map;

import io.debezium.DebeziumException;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;

/**
 * PostgreSQL Test resource with enabled vector extension
 */
public class VectorPostgresTestResourceLifecycleManager extends PostgresTestResourceLifecycleManager {

    @Override
    public Map<String, String> start() {
        final var params = super.start();

        final JdbcConfiguration config = JdbcConfiguration.create()
                .with("hostname", PostgresTestResourceLifecycleManager.POSTGRES_HOST)
                .with("port", params.get("debezium.source.database.port"))
                .with("user", PostgresTestResourceLifecycleManager.POSTGRES_USER)
                .with("password", PostgresTestResourceLifecycleManager.POSTGRES_PASSWORD)
                .with("dbname", "postgres")
                .build();

        try (PostgresConnection connection = new PostgresConnection(config, "Debezium Milvus Test")) {
            connection.execute(
                    "CREATE SCHEMA IF NOT EXISTS pgvector",
                    "CREATE EXTENSION IF NOT EXISTS vector SCHEMA pgvector",
                    "CREATE TABLE inventory.t_vector (pk INT8 PRIMARY KEY, value VARCHAR(32), f_vector pgvector.vector(3), f_json JSON);",
                    "INSERT INTO inventory.t_vector VALUES (1, 'one', '[1.1, 1.2, 1.3]', '{}'::JSON)",
                    "INSERT INTO inventory.t_vector VALUES (2, 'two', '[2.1, 2.2, 2.3]', '{}'::JSON)");
        }
        catch (Exception e) {
            throw new DebeziumException(e);
        }

        return params;
    }

}
