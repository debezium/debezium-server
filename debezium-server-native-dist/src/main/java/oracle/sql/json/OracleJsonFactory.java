/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package oracle.sql.json;

/**
 * Stub for {@code oracle.sql.json.OracleJsonFactory} from the Oracle JDBC driver (ojdbc11).
 *
 * <p>Hibernate ORM's {@code OracleOsonJdbcType} declares a static field of this type, making it
 * reachable during GraalVM native-image linking ({@code --link-at-build-time}). When the Oracle
 * JDBC driver is excluded from the build, the linker fails to resolve the type. This minimal
 * stub satisfies the linker; the Oracle dialect code paths are never executed at runtime when
 * using a non-Oracle database for the debezium-server-jdbc
 */
public class OracleJsonFactory {
    public OracleJsonFactory() {
    }
}
