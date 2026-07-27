/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Kafka Connect SMT that serializes selected record-value fields to JSON strings.
 * <p>
 * Databricks Zerobus only accepts a JSON <em>string</em> for a {@code VARIANT} column; it rejects a
 * native struct/array/map ("invalid type: map/sequence, expected a string"). When flattening a
 * document (e.g. the MongoDB {@code ExtractNewDocumentState} SMT), nested fields arrive as native
 * structs/arrays, which cannot be written to a VARIANT column directly.
 * <p>
 * This SMT converts the configured fields into their JSON-string representation, so scalar fields
 * can stay strongly typed while a single schema-flexible field (e.g. {@code properties}) is written
 * to a VARIANT column. Fields that are already strings, or absent, are left untouched.
 * <p>
 * Configuration: {@code fields} — comma-separated list of top-level value field names to stringify.
 * <p>
 * Error behavior: if a targeted field cannot be serialized to JSON, the SMT logs the offending
 * record's context (field name, schema type/name, value class and value) at ERROR level and
 * re-throws a {@link ConnectException} (fail-fast). The stream halts and the offset is not
 * committed past the record, so nothing is dropped silently; set {@code errors.tolerance=all} on
 * the engine if you prefer to skip such records instead.
 */
public class StringifyFields<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(StringifyFields.class);

    public static final String FIELDS_CONFIG = "fields";

    private static final ConfigDef CONFIG_DEF = new ConfigDef().define(
            FIELDS_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
            "Comma-separated list of value field names to serialize to a JSON string.");

    private Set<String> fields;
    private final JsonConverter jsonConverter = new JsonConverter();

    @Override
    public void configure(Map<String, ?> configs) {
        // Debezium/Kafka Connect passes SMT config values as raw strings (not parsed
        // through ConfigDef), so accept both a List and a comma-separated String.
        Object raw = configs.get(FIELDS_CONFIG);
        this.fields = new LinkedHashSet<>();
        if (raw instanceof List) {
            for (Object f : (List<?>) raw) {
                this.fields.add(f.toString().trim());
            }
        }
        else if (raw != null) {
            for (String f : raw.toString().split(",")) {
                if (!f.trim().isEmpty()) {
                    this.fields.add(f.trim());
                }
            }
        }
        // schemas.enable=false → emit plain JSON without the Connect schema envelope.
        Map<String, Object> jc = new HashMap<>();
        jc.put("schemas.enable", false);
        jc.put("converter.type", "value");
        jsonConverter.configure(jc);
    }

    @Override
    public R apply(R record) {
        if (record.value() == null || !(record.value() instanceof Struct)) {
            return record;
        }
        Struct value = (Struct) record.value();
        Schema schema = value.schema();

        // Build a new schema where the targeted fields become STRING.
        SchemaBuilder builder = SchemaBuilder.struct().name(schema.name()).version(schema.version());
        for (Field field : schema.fields()) {
            if (fields.contains(field.name())) {
                builder.field(field.name(), field.schema().isOptional() ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA);
            }
            else {
                builder.field(field.name(), field.schema());
            }
        }
        Schema newSchema = builder.build();

        Struct newValue = new Struct(newSchema);
        for (Field field : schema.fields()) {
            Object fieldValue = value.get(field);
            if (fields.contains(field.name()) && fieldValue != null && !(fieldValue instanceof String)) {
                newValue.put(field.name(), toJsonString(field.name(), field.schema(), fieldValue));
            }
            else {
                newValue.put(field.name(), fieldValue);
            }
        }

        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                newSchema, newValue, record.timestamp(), record.headers());
    }

    /** Serialize a single field value to its JSON representation using the Connect JsonConverter. */
    private String toJsonString(String fieldName, Schema fieldSchema, Object fieldValue) {
        try {
            byte[] json = ((Converter) jsonConverter).fromConnectData("_stringify", fieldSchema, fieldValue);
            return new String(json, java.nio.charset.StandardCharsets.UTF_8);
        }
        catch (Exception e) {
            // Fail-fast: a field that cannot be serialized must stop the stream rather than be
            // dropped or corrupted silently. Log enough context to diagnose the offending record
            // (field name, schema type/name and value class), then re-throw so the engine halts
            // and the offset is not committed past this record.
            String schemaType = fieldSchema == null ? "null" : String.valueOf(fieldSchema.type());
            String schemaName = fieldSchema == null ? "null" : String.valueOf(fieldSchema.name());
            String valueClass = fieldValue == null ? "null" : fieldValue.getClass().getName();
            LOGGER.error("StringifyFields failed to serialize field '{}' (schema type={}, schema name={}, "
                    + "value class={}) to a JSON string. The record is not modified and the stream will halt "
                    + "so no data is dropped or committed past this point. Value: {}",
                    fieldName, schemaType, schemaName, valueClass, fieldValue, e);
            throw new ConnectException("StringifyFields could not serialize field '" + fieldName
                    + "' (schema type=" + schemaType + ", value class=" + valueClass + ") to a JSON string", e);
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        jsonConverter.close();
    }
}
