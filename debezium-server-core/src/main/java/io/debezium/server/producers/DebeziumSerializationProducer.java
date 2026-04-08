package io.debezium.server.producers;

import static io.debezium.server.configuration.DebeziumProperties.PROP_HEADER_FORMAT;
import static io.debezium.server.configuration.DebeziumProperties.PROP_KEY_FORMAT;
import static io.debezium.server.configuration.DebeziumProperties.PROP_VALUE_FORMAT;

import io.quarkus.arc.Unremovable;
import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import org.eclipse.microprofile.config.Config;

import io.debezium.server.configuration.DebeziumConfiguration;
import io.debezium.DebeziumException;
import io.debezium.embedded.ClientProvided;
import io.debezium.embedded.Connect;
import io.debezium.engine.format.Avro;
import io.debezium.engine.format.Binary;
import io.debezium.engine.format.CloudEvents;
import io.debezium.engine.format.Json;
import io.debezium.engine.format.JsonByteArray;
import io.debezium.engine.format.Protobuf;
import io.debezium.engine.format.SerializationFormat;
import io.debezium.engine.format.SimpleString;
import io.debezium.runtime.DebeziumSerialization;

/**
 * CDI producer that creates the {@link DebeziumSerialization} configuration for change event serialization.
 * <p>
 * This producer reads serialization format configuration from application properties and maps them
 * to the appropriate {@link SerializationFormat} classes used by the Debezium embedded engine.
 * The following configuration properties are supported:
 * <ul>
 *   <li>{@code debezium.format.key} - Serialization format for event keys (default: json)</li>
 *   <li>{@code debezium.format.value} - Serialization format for event values (default: json)</li>
 *   <li>{@code debezium.format.header} - Serialization format for event headers (default: json)</li>
 * </ul>
 * <p>
 * Supported formats for keys and values:
 * {@code json}, {@code jsonbytearray}, {@code cloudevents}, {@code avro}, {@code protobuf},
 * {@code binary}, {@code simplestring}, {@code connect}, {@code clientprovided}
 * <p>
 * Supported formats for headers:
 * {@code json}, {@code jsonbytearray}, {@code connect}, {@code clientprovided}
 * <p>
 * If an unknown format is specified, a {@link DebeziumException} is thrown at application startup.
 *
 * @see DebeziumSerialization
 * @see SerializationFormat
 */
@ApplicationScoped
public class DebeziumSerializationProducer {

    private static final String FORMAT_JSON = Json.class.getSimpleName().toLowerCase();
    private static final String FORMAT_JSON_BYTE_ARRAY = JsonByteArray.class.getSimpleName().toLowerCase();
    private static final String FORMAT_CLOUDEVENT = CloudEvents.class.getSimpleName().toLowerCase();
    private static final String FORMAT_AVRO = Avro.class.getSimpleName().toLowerCase();
    private static final String FORMAT_PROTOBUF = Protobuf.class.getSimpleName().toLowerCase();
    private static final String FORMAT_BINARY = Binary.class.getSimpleName().toLowerCase();
    private static final String FORMAT_STRING = SimpleString.class.getSimpleName().toLowerCase();
    private static final String FORMAT_CONNECT = Connect.class.getSimpleName().toLowerCase();
    private static final String FORMAT_CLIENT_PROVIDED = ClientProvided.class.getSimpleName().toLowerCase();

    @Startup
    @Produces
    @Unremovable
    @ApplicationScoped
    public DebeziumSerialization<Object, Object, Object> produces() {
        Config config = DebeziumConfiguration.get();

        return new DebeziumSerialization<>() {
            @Override
            public Class<? extends SerializationFormat<Object>> getKeyFormat() {
                return (Class<? extends SerializationFormat<Object>>) getFormat(config, PROP_KEY_FORMAT);
            }

            @Override
            public Class<? extends SerializationFormat<Object>> getValueFormat() {
                return (Class<? extends SerializationFormat<Object>>) getFormat(config, PROP_VALUE_FORMAT);
            }

            @Override
            public Class<? extends SerializationFormat<Object>> getHeaderFormat() {
                return (Class<? extends SerializationFormat<Object>>) getHeader(config);
            }

            @Override
            public String getEngineId() {
                /**
                 * TODO: we don't support for now multi-engine serialization
                 */
                return "default";
            }
        };
    }

    private Class<? extends SerializationFormat<?>> getFormat(Config config, String property) {
        final String formatName = config.getOptionalValue(property, String.class).orElse(FORMAT_JSON);

        if (FORMAT_JSON.equals(formatName)) {
            return Json.class;
        }
        if (FORMAT_JSON_BYTE_ARRAY.equals(formatName)) {
            return JsonByteArray.class;
        }
        if (FORMAT_CLOUDEVENT.equals(formatName)) {
            return CloudEvents.class;
        }
        if (FORMAT_AVRO.equals(formatName)) {
            return Avro.class;
        }
        if (FORMAT_PROTOBUF.equals(formatName)) {
            return Protobuf.class;
        }
        if (FORMAT_BINARY.equals(formatName)) {
            return Binary.class;
        }
        if (FORMAT_STRING.equals(formatName)) {
            return SimpleString.class;
        }
        if (FORMAT_CONNECT.equalsIgnoreCase(formatName)) {
            return Connect.class;
        }
        if (FORMAT_CLIENT_PROVIDED.equals(formatName)) {
            return ClientProvided.class;
        }

        throw new DebeziumException("Unknown format '" + formatName + "' for option " + "'" + property + "'");
    }

    private Class<? extends SerializationFormat<?>> getHeader(Config config) {
        final String formatName = config.getOptionalValue(PROP_HEADER_FORMAT, String.class).orElse(FORMAT_JSON);

        if (FORMAT_JSON.equals(formatName)) {
            return Json.class;
        }
        if (FORMAT_JSON_BYTE_ARRAY.equals(formatName)) {
            return JsonByteArray.class;
        }
        if (FORMAT_CONNECT.equals(formatName)) {
            return Connect.class;
        }
        if (FORMAT_CLIENT_PROVIDED.equals(formatName)) {
            return ClientProvided.class;
        }

        throw new DebeziumException("Unknown format '" + formatName + "' for option " + "'" + PROP_HEADER_FORMAT + "'");
    }

}
