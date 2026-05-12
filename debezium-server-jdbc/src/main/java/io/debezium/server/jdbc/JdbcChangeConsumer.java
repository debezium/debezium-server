/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.jdbc;

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;

import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.Module;
import io.debezium.config.Field;
import io.debezium.connector.jdbc.JdbcChangeEventSink;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.QueryBinderResolver;
import io.debezium.connector.jdbc.RecordWriter;
import io.debezium.connector.jdbc.UnnestRecordWriter;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.dialect.DatabaseDialectResolver;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataFactory;
import io.debezium.openlineage.ConnectorContext;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.api.DebeziumServerSink;

/**
 * Implementation of the consumer that delivers change events to a JDBC database
 * using the Debezium JDBC connector.
 * <p>
 * This consumer wraps JdbcChangeEventSink and manages the complete lifecycle including:
 * - Hibernate SessionFactory creation and cleanup
 * - StatelessSession management
 * - Database dialect resolution
 * - Configuration transformation from debezium.sink.jdbc.* properties
 * - Conversion from ChangeEvent to SinkRecord
 * <p>
 * The consumer is responsible for SessionFactory lifecycle because JdbcChangeEventSink
 * expects a session to be provided but does not manage the factory itself.
 *
 * @author Mario Fiore Vitale
 */
@Named("jdbc")
@Dependent
public class JdbcChangeConsumer extends BaseChangeConsumer
        implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>>, DebeziumServerSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcChangeConsumer.class);
    private static final String PROP_PREFIX = "debezium.sink.jdbc.";

    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();
    private final ChangeEventToSinkRecordConverter converter = new ChangeEventToSinkRecordConverter();

    // Lifecycle managed components
    private SessionFactory sessionFactory;
    private StatelessSession session;
    private JdbcChangeEventSink changeEventSink;
    private JdbcChangeConsumerConfig config;

    @PostConstruct
    void connect() {
        LOGGER.info("Initializing JDBC sink");

        try {
            Config mpConfig = ConfigProvider.getConfig();
            io.debezium.config.Configuration configuration = io.debezium.config.Configuration.from(getConfigSubset(mpConfig, PROP_PREFIX));

            this.config = new JdbcChangeConsumerConfig(configuration);
            JdbcSinkConnectorConfig jdbcConfig = config.getJdbcConfig();

            jdbcConfig.validate();

            org.hibernate.cfg.Configuration hibernateConfig = jdbcConfig.getHibernateConfiguration();
            String connectionUrl = hibernateConfig.getProperty(org.hibernate.cfg.AvailableSettings.JAKARTA_JDBC_URL);
            LOGGER.info("JDBC connection URL: {}", connectionUrl);

            LOGGER.info("Creating Hibernate SessionFactory");
            this.sessionFactory = jdbcConfig.getHibernateConfiguration().buildSessionFactory();

            LOGGER.debug("Opening Hibernate StatelessSession");
            this.session = sessionFactory.openStatelessSession();

            DatabaseDialect dialect = DatabaseDialectResolver.resolve(jdbcConfig, sessionFactory);
            LOGGER.info("Resolved database dialect: {}", dialect.getClass().getSimpleName());

            QueryBinderResolver queryBinderResolver = new QueryBinderResolver();

            RecordWriter recordWriter = createRecordWriter(
                    session, queryBinderResolver, jdbcConfig, dialect);

            ConnectorContext connectorContext = new ConnectorContext(
                    "debezium-server-jdbc",
                    "jdbc",
                    "0",
                    Module.version(),
                    UUID.randomUUID(),
                    new java.util.HashMap<>());

            this.changeEventSink = new JdbcChangeEventSink(
                    jdbcConfig, session, dialect, recordWriter, connectorContext);

            LOGGER.info("JDBC sink initialized successfully");
            LOGGER.info("Insert mode: {}", jdbcConfig.getInsertMode());
            LOGGER.info("Schema evolution: {}", jdbcConfig.getSchemaEvolutionMode());
            LOGGER.info("Batch size: {}", jdbcConfig.getBatchSize());

        }
        catch (Exception e) {
            LOGGER.error("Failed to initialize JDBC sink", e);
            close();
            throw new DebeziumException("Failed to initialize JDBC sink", e);
        }
    }

    private RecordWriter createRecordWriter(
                                            StatelessSession session,
                                            QueryBinderResolver queryBinderResolver,
                                            JdbcSinkConnectorConfig config,
                                            DatabaseDialect dialect) {

        return new UnnestRecordWriter(session, queryBinderResolver, config, dialect);
    }

    @Override
    public void handleBatch(
                            List<ChangeEvent<Object, Object>> records,
                            DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer) {

        LOGGER.debug("Processing batch of {} records", records.size());

        try {
            Collection<SinkRecord> sinkRecords = records.stream()
                    .map(converter::convert)
                    .collect(Collectors.toList());

            changeEventSink.execute(sinkRecords);

            for (ChangeEvent<Object, Object> record : records) {
                committer.markProcessed(record);
            }

            committer.markBatchFinished();

            LOGGER.debug("Successfully processed batch of {} records", records.size());

        }
        catch (Exception e) {
            LOGGER.error("Failed to process batch of {} records", records.size(), e);
            throw new DebeziumException("Failed to process batch", e);
        }
    }

    @PreDestroy
    @Override
    public void close() {
        LOGGER.info("Closing JDBC sink");

        if (changeEventSink != null) {
            try {
                LOGGER.debug("Closing JdbcChangeEventSink");
                changeEventSink.close();
            }
            catch (Exception e) {
                LOGGER.warn("Error closing JdbcChangeEventSink", e);
            }
            finally {
                changeEventSink = null;
            }
        }

        if (session != null && session.isOpen()) {
            try {
                LOGGER.debug("Closing StatelessSession");
                session.close();
            }
            catch (Exception e) {
                LOGGER.warn("Error closing session", e);
            }
            finally {
                session = null;
            }
        }

        if (sessionFactory != null && !sessionFactory.isClosed()) {
            try {
                LOGGER.info("Closing Hibernate SessionFactory");
                sessionFactory.close();
            }
            catch (Exception e) {
                LOGGER.warn("Error closing SessionFactory", e);
            }
            finally {
                sessionFactory = null;
            }
        }

        LOGGER.info("JDBC sink closed");
    }

    @Override
    public Field.Set getConfigFields() {
        if (config != null) {
            return config.getAllConfigurationFields();
        }
        return JdbcSinkConnectorConfig.ALL_FIELDS;
    }

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(componentMetadataFactory.createComponentMetadata(this, Module.version()));
    }
}
