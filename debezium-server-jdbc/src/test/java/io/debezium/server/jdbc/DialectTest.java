/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.jdbc;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.Set;
import java.util.stream.Collectors;

import org.hibernate.engine.jdbc.dialect.spi.DialectResolver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;

import io.debezium.connector.jdbc.dialect.DatabaseDialectProvider;
import io.debezium.connector.jdbc.dialect.cockroachdb.CockroachDBDatabaseDialect;
import io.debezium.connector.jdbc.dialect.db2.Db2DatabaseDialect;
import io.debezium.connector.jdbc.dialect.db2i.Db2iDatabaseDialect;
import io.debezium.connector.jdbc.dialect.mysql.MariaDbDatabaseDialect;
import io.debezium.connector.jdbc.dialect.mysql.MySqlDatabaseDialect;
import io.debezium.connector.jdbc.dialect.oracle.OracleDatabaseDialect;
import io.debezium.connector.jdbc.dialect.postgres.PostgresDatabaseDialect;
import io.debezium.connector.jdbc.dialect.singlestore.SingleStoreDatabaseDialect;
import io.debezium.connector.jdbc.dialect.sqlserver.SqlServerDatabaseDialect;
import io.debezium.connector.jdbc.dialect.starrocks.StarRocksDatabaseDialect;
import io.debezium.connector.jdbc.dialect.starrocks.StarRocksDialectResolver;

public class DialectTest {

    public static final String DIALECT_PACKAGE = "io.debezium.connector.jdbc.dialect";
    private static final JavaClasses CLASSES = new ClassFileImporter().importPackages(DIALECT_PACKAGE);

    @Test
    @DisplayName("should fail if a new DialectProvider is not registered for reflection")
    void shouldFailWhenDialectProviderIsNotRegisteredForReflection() {
        assertThat(CLASSES.stream()
                .filter(c -> c.isAssignableTo(DatabaseDialectProvider.class))
                .filter(c -> !c.isInterface())
                .map(JavaClass::reflect)
                .collect(Collectors.toSet()))
                .as("New implementations of DatabaseDialectProvider require registration in JdbcSinkReflectionConfiguration")
                .isEqualTo(Set.<Class<?>> of(
                        MySqlDatabaseDialect.MySqlDatabaseDialectProvider.class,
                        MariaDbDatabaseDialect.MariaDbDatabaseDialectProvider.class,
                        PostgresDatabaseDialect.PostgresDatabaseDialectProvider.class,
                        OracleDatabaseDialect.OracleDatabaseDialectProvider.class,
                        SqlServerDatabaseDialect.SqlServerDatabaseDialectProvider.class,
                        Db2DatabaseDialect.Db2DatabaseProvider.class,
                        Db2iDatabaseDialect.Db2iDatabaseProvider.class,
                        StarRocksDatabaseDialect.StarRocksDatabaseDialectProvider.class,
                        SingleStoreDatabaseDialect.SingleStoreDatabaseDialectProvider.class,
                        CockroachDBDatabaseDialect.CockroachDBDatabaseDialectProvider.class));
    }

    @Test
    @DisplayName("should fail if a new DialectResolver is not registered for reflection")
    void shouldFailWhenDialectResolverIsNotRegisteredForReflection() {
        assertThat(CLASSES.stream()
                .filter(c -> c.isAssignableTo(DialectResolver.class))
                .filter(c -> !c.isInterface())
                .map(JavaClass::reflect)
                .collect(Collectors.toSet()))
                .as("New implementations of DialectResolver require registration in JdbcSinkReflectionConfiguration")
                .isEqualTo(Set.<Class<?>> of(StarRocksDialectResolver.class));
    }
}
