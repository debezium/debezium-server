/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.jdbc.quarkus;

import io.debezium.connector.jdbc.dialect.db2.Db2DatabaseDialect;
import io.debezium.connector.jdbc.dialect.db2i.Db2iDatabaseDialect;
import io.debezium.connector.jdbc.dialect.mysql.MariaDbDatabaseDialect;
import io.debezium.connector.jdbc.dialect.mysql.MySqlDatabaseDialect;
import io.debezium.connector.jdbc.dialect.oracle.OracleDatabaseDialect;
import io.debezium.connector.jdbc.dialect.postgres.PostgresDatabaseDialect;
import io.debezium.connector.jdbc.dialect.sqlserver.SqlServerDatabaseDialect;
import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection(targets = {
        MySqlDatabaseDialect.MySqlDatabaseDialectProvider.class,
        MariaDbDatabaseDialect.MariaDbDatabaseDialectProvider.class,
        PostgresDatabaseDialect.PostgresDatabaseDialectProvider.class,
        OracleDatabaseDialect.OracleDatabaseDialectProvider.class,
        SqlServerDatabaseDialect.SqlServerDatabaseDialectProvider.class,
        Db2DatabaseDialect.Db2DatabaseProvider.class,
        Db2iDatabaseDialect.Db2iDatabaseProvider.class,
})
public class JdbcSinkReflectionConfiguration {
}
