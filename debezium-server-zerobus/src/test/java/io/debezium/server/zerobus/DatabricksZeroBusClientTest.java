/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.zerobus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import com.databricks.zerobus.ArrowStreamConfigurationOptions;
import com.databricks.zerobus.StreamConfigurationOptions;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DynamicMessage;

class DatabricksZeroBusClientTest {

    @Test
    void writesJsonRecordsThroughJsonStream() throws Exception {
        FakeSdkAdapter sdk = new FakeSdkAdapter();
        ZeroBusSinkConfig config = ZeroBusSinkConfigTest.baseConfig();

        DatabricksZeroBusClient client = new DatabricksZeroBusClient(config, sdk, new ZeroBusJsonSerializer(), new ZeroBusProtobufSerializer(), null);

        ZeroBusWriteResult result = client.write("main.bronze.customers", List.of(record()));

        assertThat(result.allAcknowledged(1)).isTrue();
        assertThat(sdk.jsonRequests).containsExactly("main.bronze.customers");
        assertThat(sdk.protoRequests).isEmpty();
        assertThat(sdk.jsonStream.records).hasSize(1);
        assertThat(sdk.jsonStream.waitedOffsets).containsExactly(11L);
        client.close();
    }

    @Test
    void emptyJsonOffsetFailsInsteadOfAcknowledgingRecords() {
        FakeSdkAdapter sdk = new FakeSdkAdapter();
        sdk.jsonStream.offset = Optional.empty();
        ZeroBusSinkConfig config = ZeroBusSinkConfigTest.baseConfig();
        DatabricksZeroBusClient client = new DatabricksZeroBusClient(config, sdk, new ZeroBusJsonSerializer(), new ZeroBusProtobufSerializer(), null);

        assertThatThrownBy(() -> client.write("main.bronze.customers", List.of(record())))
                .isInstanceOf(ZeroBusRetriableException.class)
                .hasMessageContaining("did not return a durable offset");
    }

    @Test
    void writesProtobufRecordsThroughProtoStream() throws Exception {
        FakeSdkAdapter sdk = new FakeSdkAdapter();
        ZeroBusSinkConfig config = new ZeroBusSinkConfig(ZeroBusSinkConfigTest.baseBuilder()
                .with("record.format", "protobuf")
                .build());
        config.validate();
        ZeroBusProtobufSerializer serializer = new ZeroBusProtobufSerializer();

        DatabricksZeroBusClient client = new DatabricksZeroBusClient(config, sdk, new ZeroBusJsonSerializer(), serializer, null);

        ZeroBusWriteResult result = client.write("main.bronze.customers", List.of(record()));

        DynamicMessage message = DynamicMessage.parseFrom(serializer.descriptor(), sdk.protoStream.records.get(0));
        assertThat(result.allAcknowledged(1)).isTrue();
        assertThat(sdk.jsonRequests).isEmpty();
        assertThat(sdk.protoRequests).containsExactly("main.bronze.customers");
        assertThat(sdk.protoDescriptor.getName()).isEqualTo("DebeziumZeroBusRecord");
        assertThat(message.getField(serializer.descriptor().findFieldByName("operation"))).isEqualTo("update");
        assertThat(message.getField(serializer.descriptor().findFieldByName("value"))).isEqualTo("{\"after\":{\"id\":1}}");
        assertThat(sdk.protoStream.waitedOffsets).containsExactly(19L);
        client.close();
    }

    @Test
    void writesArrowRecordsThroughArrowStream() throws Exception {
        FakeSdkAdapter sdk = new FakeSdkAdapter();
        ZeroBusSinkConfig config = new ZeroBusSinkConfig(ZeroBusSinkConfigTest.baseBuilder()
                .with("record.format", "arrow")
                .build());
        config.validate();
        ZeroBusArrowSerializer serializer = new ZeroBusArrowSerializer();

        DatabricksZeroBusClient client = new DatabricksZeroBusClient(config, sdk, new ZeroBusJsonSerializer(), new ZeroBusProtobufSerializer(), serializer);

        ZeroBusWriteResult result = client.write("main.bronze.customers", List.of(record()));

        assertThat(result.allAcknowledged(1)).isTrue();
        assertThat(sdk.jsonRequests).isEmpty();
        assertThat(sdk.protoRequests).isEmpty();
        assertThat(sdk.arrowRequests).containsExactly("main.bronze.customers");
        assertThat(sdk.arrowSchema.getFields()).extracting(field -> field.getName()).containsExactly(
                "target_table",
                "destination",
                "partition",
                "operation",
                "idempotency_key",
                "key",
                "value",
                "source_position",
                "headers");
        assertThat(sdk.arrowStream.records).containsExactly("{\"after\":{\"id\":1}}");
        assertThat(sdk.arrowStream.waitedOffsets).containsExactly(23L);
        client.close();
    }

    private static ZeroBusRecord record() {
        return new ZeroBusRecord(
                "main.bronze.customers",
                "server.inventory.customers",
                0,
                "{\"id\":1}",
                "{\"after\":{\"id\":1}}",
                Map.of("offset.lsn", "100"),
                Map.of("source.lsn", "100"),
                ZeroBusOperation.UPDATE,
                "stable-idempotency-key");
    }

    private static class FakeSdkAdapter implements ZeroBusSdkAdapter {
        private final FakeJsonStream jsonStream = new FakeJsonStream();
        private final FakeProtoStream protoStream = new FakeProtoStream();
        private final FakeArrowStream arrowStream = new FakeArrowStream();
        private final List<String> jsonRequests = new ArrayList<>();
        private final List<String> protoRequests = new ArrayList<>();
        private final List<String> arrowRequests = new ArrayList<>();
        private DescriptorProto protoDescriptor;
        private Schema arrowSchema;

        @Override
        public CompletableFuture<ZeroBusJsonStreamAdapter> createJsonStream(String targetTable,
                                                                            String clientId,
                                                                            String clientSecret,
                                                                            StreamConfigurationOptions options) {
            jsonRequests.add(targetTable);
            return CompletableFuture.completedFuture(jsonStream);
        }

        @Override
        public CompletableFuture<ZeroBusProtoStreamAdapter> createProtoStream(String targetTable,
                                                                              DescriptorProto descriptor,
                                                                              String clientId,
                                                                              String clientSecret,
                                                                              StreamConfigurationOptions options) {
            protoRequests.add(targetTable);
            protoDescriptor = descriptor;
            return CompletableFuture.completedFuture(protoStream);
        }

        @Override
        public CompletableFuture<ZeroBusArrowStreamAdapter> createArrowStream(String targetTable,
                                                                              Schema schema,
                                                                              String clientId,
                                                                              String clientSecret,
                                                                              ArrowStreamConfigurationOptions options) {
            arrowRequests.add(targetTable);
            arrowSchema = schema;
            return CompletableFuture.completedFuture(arrowStream);
        }

        @Override
        public void close() {
        }
    }

    private static class FakeJsonStream implements ZeroBusJsonStreamAdapter {
        private final List<String> records = new ArrayList<>();
        private final List<Long> waitedOffsets = new ArrayList<>();
        private Optional<Long> offset = Optional.of(11L);

        @Override
        public Optional<Long> ingestRecordsOffset(List<String> records) {
            this.records.addAll(records);
            return offset;
        }

        @Override
        public void waitForOffset(long offset) {
            waitedOffsets.add(offset);
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void close() {
        }
    }

    private static class FakeProtoStream implements ZeroBusProtoStreamAdapter {
        private final List<byte[]> records = new ArrayList<>();
        private final List<Long> waitedOffsets = new ArrayList<>();

        @Override
        public Optional<Long> ingestRecordsOffset(List<byte[]> records) {
            this.records.addAll(records);
            return Optional.of(19L);
        }

        @Override
        public void waitForOffset(long offset) {
            waitedOffsets.add(offset);
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void close() {
        }
    }

    private static class FakeArrowStream implements ZeroBusArrowStreamAdapter {
        private final List<String> records = new ArrayList<>();
        private final List<Long> waitedOffsets = new ArrayList<>();

        @Override
        public Optional<Long> ingestBatch(VectorSchemaRoot root) {
            VarCharVector value = (VarCharVector) root.getVector("value");
            for (int row = 0; row < root.getRowCount(); row++) {
                records.add(value.getObject(row).toString());
            }
            return Optional.of(23L);
        }

        @Override
        public void waitForOffset(long offset) {
            waitedOffsets.add(offset);
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void close() {
        }
    }
}
