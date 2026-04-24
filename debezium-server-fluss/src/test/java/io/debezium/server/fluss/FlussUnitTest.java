/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.fluss;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.Append;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.Upsert;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine.RecordCommitter;

/**
 * Unit tests for Apache Fluss.
 *
 * @author Chris Cranford
 */
public class FlussUnitTest {

    private static final String BOOTSTRAP_SERVERS = "localhost:9123";
    private static final String DEFAULT_DATABASE = "testdb";
    private static final String TABLE_NAME = "customers";
    private static final String TABLE_NAME_2 = "orders";
    private static final TablePath TABLE_PATH = TablePath.of(DEFAULT_DATABASE, TABLE_NAME);
    private static final TablePath TABLE_PATH_2 = TablePath.of(DEFAULT_DATABASE, TABLE_NAME_2);

    private static final org.apache.kafka.connect.data.Schema ROW_SCHEMA = SchemaBuilder.struct()
            .field("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
            .field("name", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
            .optional()
            .build();

    private static final org.apache.kafka.connect.data.Schema ROW_SCHEMA_WITH_NULLABLE = SchemaBuilder.struct()
            .field("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
            .field("name", SchemaBuilder.string().optional().build())
            .optional()
            .build();

    private static final org.apache.kafka.connect.data.Schema ENVELOPE_SCHEMA = SchemaBuilder.struct()
            .name("test.Envelope")
            .field(Envelope.FieldName.BEFORE, ROW_SCHEMA)
            .field(Envelope.FieldName.AFTER, ROW_SCHEMA)
            .field(Envelope.FieldName.OPERATION, org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
            .build();

    private static final org.apache.kafka.connect.data.Schema FLAT_SCHEMA = SchemaBuilder.struct()
            .name("test.Value")
            .field("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
            .field("name", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
            .build();

    private FlussChangeConsumer consumer;
    private Admin mockAdmin;
    private AppendWriter mockAppendWriter;
    private UpsertWriter mockUpsertWriter;
    private RecordCommitter<ChangeEvent<Object, Object>> committer;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setup() {
        Connection mockConnection = mock(Connection.class);
        Table mockTable = mock(Table.class);

        mockAdmin = mock(Admin.class);
        mockAppendWriter = mock(AppendWriter.class);
        mockUpsertWriter = mock(UpsertWriter.class);

        Append mockAppend = mock(Append.class);
        Upsert mockUpsert = mock(Upsert.class);

        when(mockConnection.getAdmin()).thenReturn(mockAdmin);
        when(mockConnection.getTable(any(TablePath.class))).thenReturn(mockTable);
        when(mockTable.newAppend()).thenReturn(mockAppend);
        when(mockTable.newUpsert()).thenReturn(mockUpsert);
        when(mockAppend.createWriter()).thenReturn(mockAppendWriter);
        when(mockUpsert.createWriter()).thenReturn(mockUpsertWriter);

        committer = mock(RecordCommitter.class);

        consumer = new FlussChangeConsumer();
        consumer.config = new FlussChangeConsumerConfig(Configuration.from(Map.of(
                "bootstrap.servers", BOOTSTRAP_SERVERS,
                "default.database", DEFAULT_DATABASE)));
        consumer.connection = mockConnection;
        consumer.admin = mockAdmin;
    }

    @Test
    public void testEmptyBatchIsHandledGracefully() throws Exception {
        consumer.handleBatch(List.of(), committer);
        verify(committer).markBatchFinished();
        verify(committer, never()).markProcessed(any());
    }

    @Test
    public void testInsertEventWrittenToLogTable() throws Exception {
        setupLogTableDescriptor(TABLE_PATH);

        final List<ChangeEvent<Object, Object>> events = List.of(
                createEnvelopeEvent(TABLE_NAME, null, rowStruct(1, "Alice"), Envelope.Operation.CREATE));

        consumer.handleBatch(events, committer);

        verify(mockAppendWriter).append(any(InternalRow.class));
        verify(mockAppendWriter).flush();
        verify(committer).markProcessed(events.getFirst());
        verify(committer).markBatchFinished();
    }

    @Test
    public void testReadEventWrittenToLogTable() throws Exception {
        setupLogTableDescriptor(TABLE_PATH);

        final List<ChangeEvent<Object, Object>> events = List.of(
                createEnvelopeEvent(TABLE_NAME, null, rowStruct(1, "Alice"), Envelope.Operation.READ));

        consumer.handleBatch(events, committer);

        verify(mockAppendWriter).append(any(InternalRow.class));
        verify(mockAppendWriter).flush();
        verify(committer).markProcessed(events.getFirst());
    }

    @Test
    public void testDeleteEventSkippedForLogTable() throws Exception {
        setupLogTableDescriptor(TABLE_PATH);

        final List<ChangeEvent<Object, Object>> events = List.of(
                createEnvelopeEvent(TABLE_NAME, rowStruct(1, "Alice"), null, Envelope.Operation.DELETE));

        consumer.handleBatch(events, committer);

        verify(mockAppendWriter, never()).append(any());
        verify(committer).markProcessed(events.getFirst());
        verify(committer).markBatchFinished();
    }

    @Test
    public void testTombstoneSkippedForLogTable() throws Exception {
        setupLogTableDescriptor(TABLE_PATH);

        consumer.handleBatch(List.of(createTombstone(TABLE_NAME)), committer);

        verify(mockAppendWriter, never()).append(any());
        verify(committer).markBatchFinished();
    }

    @Test
    public void testCreateEventWrittenToPrimaryKeyTable() throws Exception {
        setupPrimaryKeyTableDescriptor(TABLE_PATH);

        final List<ChangeEvent<Object, Object>> events = List.of(
                createEnvelopeEvent(TABLE_NAME, null, rowStruct(1, "Alice"), Envelope.Operation.CREATE));

        consumer.handleBatch(events, committer);

        verify(mockUpsertWriter).upsert(any(InternalRow.class));
        verify(mockUpsertWriter).flush();
        verify(committer).markProcessed(events.getFirst());
    }

    @Test
    public void testReadEventWrittenToPrimaryKeyTable() throws Exception {
        setupPrimaryKeyTableDescriptor(TABLE_PATH);

        final List<ChangeEvent<Object, Object>> events = List.of(
                createEnvelopeEvent(TABLE_NAME, null, rowStruct(1, "Alice"), Envelope.Operation.READ));

        consumer.handleBatch(events, committer);

        verify(mockUpsertWriter).upsert(any(InternalRow.class));
        verify(mockUpsertWriter).flush();
        verify(committer).markProcessed(events.getFirst());
    }

    @Test
    public void testUpdateEventWrittenToPrimaryKeyTable() throws Exception {
        setupPrimaryKeyTableDescriptor(TABLE_PATH);

        final List<ChangeEvent<Object, Object>> events = List.of(
                createEnvelopeEvent(TABLE_NAME, rowStruct(1, "Alice"), rowStruct(1, "Bob"), Envelope.Operation.UPDATE));

        consumer.handleBatch(events, committer);

        verify(mockUpsertWriter).upsert(any(InternalRow.class));
        verify(mockUpsertWriter).flush();
        verify(committer).markProcessed(events.getFirst());
    }

    @Test
    public void testDeleteEventWrittenToPrimaryKeyTable() throws Exception {
        setupPrimaryKeyTableDescriptor(TABLE_PATH);

        final List<ChangeEvent<Object, Object>> events = List.of(
                createEnvelopeEvent(TABLE_NAME, rowStruct(1, "Alice"), null, Envelope.Operation.DELETE));

        consumer.handleBatch(events, committer);

        verify(mockUpsertWriter).delete(any(InternalRow.class));
        verify(mockUpsertWriter).flush();
        verify(committer).markProcessed(events.getFirst());
    }

    @Test
    public void testTombstoneSkippedForPrimaryKeyTable() throws Exception {
        setupPrimaryKeyTableDescriptor(TABLE_PATH);

        consumer.handleBatch(List.of(createTombstone(TABLE_NAME)), committer);

        verify(mockUpsertWriter, never()).upsert(any());
        verify(mockUpsertWriter, never()).delete(any());
        verify(committer).markBatchFinished();
    }

    @Test
    public void testExtractedNewRecordStateWrittenToLogTable() throws Exception {
        // ExtractNewRecordState: value schema is not an envelope
        final Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .build();
        setupDescriptor(TABLE_PATH, schema);

        final Struct flatValue = new Struct(FLAT_SCHEMA).put("id", 1).put("name", "Alice");
        final List<ChangeEvent<Object, Object>> events = List.of(createFlatEvent(TABLE_NAME, flatValue));

        consumer.handleBatch(events, committer);

        verify(mockAppendWriter).append(any(InternalRow.class));
        verify(mockAppendWriter).flush();
        verify(committer).markProcessed(events.getFirst());
        verify(committer).markBatchFinished();
    }

    @Test
    public void testNullFieldValueInRowIsHandled() throws Exception {
        final Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .build();
        setupDescriptor(TABLE_PATH, schema);

        // row with a null name field
        final Struct row = new Struct(ROW_SCHEMA_WITH_NULLABLE).put("id", 1).put("name", null);
        final org.apache.kafka.connect.data.Schema envelopeWithNullable = SchemaBuilder.struct()
                .name("test.Envelope")
                .field(Envelope.FieldName.BEFORE, ROW_SCHEMA_WITH_NULLABLE)
                .field(Envelope.FieldName.AFTER, ROW_SCHEMA_WITH_NULLABLE)
                .field(Envelope.FieldName.OPERATION, org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                .build();
        final Struct value = new Struct(envelopeWithNullable)
                .put(Envelope.FieldName.BEFORE, null)
                .put(Envelope.FieldName.AFTER, row)
                .put(Envelope.FieldName.OPERATION, Envelope.Operation.CREATE.code());
        SourceRecord sr = new SourceRecord(null, null, TABLE_NAME, 0, null, null, envelopeWithNullable, value);

        final List<ChangeEvent<Object, Object>> events = List.of(createEventFromSourceRecord(TABLE_NAME, value, sr));

        consumer.handleBatch(events, committer);

        verify(mockAppendWriter).append(any(InternalRow.class));
        verify(committer).markProcessed(events.getFirst());
    }

    @Test
    public void testMultipleDestinationsInSameBatch() throws Exception {
        setupLogTableDescriptor(TABLE_PATH);
        setupLogTableDescriptor(TABLE_PATH_2);

        final List<ChangeEvent<Object, Object>> events = List.of(
                createEnvelopeEvent(TABLE_NAME, null, rowStruct(1, "Alice"), Envelope.Operation.CREATE),
                createEnvelopeEvent(TABLE_NAME_2, null, rowStruct(2, "Order"), Envelope.Operation.CREATE));

        consumer.handleBatch(events, committer);

        verify(mockAppendWriter, times(2)).append(any(InternalRow.class));
        verify(mockAppendWriter, times(2)).flush(); // once per destination group
        verify(committer, times(2)).markProcessed(any());
        verify(committer).markBatchFinished();
    }

    @Test
    public void testMultipleRecordsForSameTable() throws Exception {
        setupLogTableDescriptor(TABLE_PATH);

        final List<ChangeEvent<Object, Object>> events = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            events.add(createEnvelopeEvent(TABLE_NAME, null, rowStruct(i, "User" + i), Envelope.Operation.CREATE));
        }

        consumer.handleBatch(events, committer);

        verify(mockAppendWriter, times(5)).append(any(InternalRow.class));
        verify(mockAppendWriter, times(1)).flush();
        verify(committer, times(5)).markProcessed(any());
        verify(committer).markBatchFinished();
    }

    private void setupLogTableDescriptor(TablePath tablePath) throws Exception {
        final Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .build();
        setupDescriptor(tablePath, schema);
    }

    @SuppressWarnings("SameParameterValue")
    private void setupPrimaryKeyTableDescriptor(TablePath tablePath) throws Exception {
        final Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .primaryKey("id")
                .build();
        setupDescriptor(tablePath, schema);
    }

    private void setupDescriptor(TablePath tablePath, Schema schema) throws Exception {
        final TableDescriptor descriptor = TableDescriptor.builder().schema(schema).build();
        final TableInfo tableInfo = mock(TableInfo.class);

        when(tableInfo.toTableDescriptor()).thenReturn(descriptor);
        when(mockAdmin.getTableInfo(tablePath)).thenReturn(CompletableFuture.completedFuture(tableInfo));
    }

    private static Struct rowStruct(int id, String name) {
        return new Struct(ROW_SCHEMA).put("id", id).put("name", name);
    }

    private static ChangeEvent<Object, Object> createEnvelopeEvent(String destination,
                                                                   Struct before, Struct after,
                                                                   Envelope.Operation op) {
        final Struct value = new Struct(ENVELOPE_SCHEMA)
                .put(Envelope.FieldName.BEFORE, before)
                .put(Envelope.FieldName.AFTER, after)
                .put(Envelope.FieldName.OPERATION, op.code());

        final SourceRecord sourceRecord = new SourceRecord(null, null, destination, 0, null, null, ENVELOPE_SCHEMA, value);

        return createEventFromSourceRecord(destination, value, sourceRecord);
    }

    @SuppressWarnings("SameParameterValue")
    private static ChangeEvent<Object, Object> createFlatEvent(String destination, Struct value) {
        final SourceRecord sourceRecord = new SourceRecord(null, null, destination, 0,
                null, null, FLAT_SCHEMA, value);

        return createEventFromSourceRecord(destination, value, sourceRecord);
    }

    @SuppressWarnings({ "unchecked", "SameParameterValue" })
    private static ChangeEvent<Object, Object> createTombstone(String destination) {
        final SourceRecord sourceRecord = new SourceRecord(null, null, destination, 0,
                null, null, null, null);

        final EmbeddedEngineChangeEvent<Object, Object, SourceRecord> event = mock(EmbeddedEngineChangeEvent.class);

        when(event.destination()).thenReturn(destination);
        when(event.value()).thenReturn(null);
        when(event.sourceRecord()).thenReturn(sourceRecord);

        return event;
    }

    @SuppressWarnings("unchecked")
    private static ChangeEvent<Object, Object> createEventFromSourceRecord(String destination,
                                                                           Object value,
                                                                           SourceRecord sourceRecord) {
        final EmbeddedEngineChangeEvent<Object, Object, SourceRecord> event = mock(EmbeddedEngineChangeEvent.class);

        when(event.destination()).thenReturn(destination);
        when(event.value()).thenReturn(value);
        when(event.sourceRecord()).thenReturn(sourceRecord);

        return event;
    }
}
