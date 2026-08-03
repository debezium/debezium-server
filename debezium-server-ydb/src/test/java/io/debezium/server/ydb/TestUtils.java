/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ydb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.awaitility.Awaitility;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Strings;

import tech.ydb.auth.NopAuthProvider;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Result;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.description.Codec;
import tech.ydb.topic.description.Consumer;
import tech.ydb.topic.description.SupportedCodecs;
import tech.ydb.topic.description.TopicDescription;
import tech.ydb.topic.read.AsyncReader;
import tech.ydb.topic.read.Message;
import tech.ydb.topic.read.events.AbstractReadEventHandler;
import tech.ydb.topic.read.events.DataReceivedEvent;
import tech.ydb.topic.read.events.StartPartitionSessionEvent;
import tech.ydb.topic.settings.CreateTopicSettings;
import tech.ydb.topic.settings.PartitioningSettings;
import tech.ydb.topic.settings.ReadEventHandlersSettings;
import tech.ydb.topic.settings.ReaderSettings;
import tech.ydb.topic.settings.TopicReadSettings;

public final class TestUtils {

    private TestUtils() {
    }

    public static int waitTimeForRecords() {
        return Integer.parseInt(System.getProperty("test.wait.for.records", "60"));
    }

    public static void waitBoolean(Supplier<Boolean> condition) {
        Awaitility.await()
                .atMost(Duration.ofSeconds(waitTimeForRecords()))
                .pollInterval(Duration.ofMillis(500))
                .ignoreExceptions()
                .until(() -> Boolean.TRUE.equals(condition.get()));
    }

    public static PostgresConnection getPostgresConnection() {
        return new PostgresConnection(JdbcConfiguration.create()
                .with("hostname", PostgresTestResourceLifecycleManager.POSTGRES_HOST)
                .with("port", PostgresTestResourceLifecycleManager.getContainer()
                        .getMappedPort(PostgresTestResourceLifecycleManager.POSTGRES_PORT))
                .with("user", PostgresTestResourceLifecycleManager.POSTGRES_USER)
                .with("password", PostgresTestResourceLifecycleManager.POSTGRES_PASSWORD)
                .with("dbname", PostgresTestResourceLifecycleManager.POSTGRES_DBNAME)
                .build(), "Debezium YDB Test");
    }

    public record CapturedMessage(byte[] data, byte[] key) {
    }

    public static TopicCapture captureTopic(String topicPath, String consumerName) {
        ConcurrentLinkedQueue<CapturedMessage> captured = new ConcurrentLinkedQueue<>();
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ydb-it-reader");
            t.setDaemon(true);
            return t;
        });

        GrpcTransport transport = openTransport();
        TopicClient topicClient = TopicClient.newClient(transport).build();

        ReaderSettings rs = ReaderSettings.newBuilder()
                .setConsumerName(consumerName)
                .addTopic(TopicReadSettings.newBuilder().setPath(topicPath).build())
                .build();

        ReadEventHandlersSettings hs = ReadEventHandlersSettings.newBuilder()
                .setExecutor(exec)
                .setEventHandler(new AbstractReadEventHandler() {
                    @Override
                    public void onStartPartitionSession(StartPartitionSessionEvent event) {
                        event.confirm();
                    }

                    @Override
                    public void onMessages(DataReceivedEvent event) {
                        for (Message m : event.getMessages()) {
                            byte[] keyBytes = m.getMetadataItems().stream()
                                    .filter(mi -> "key".equals(mi.getKey()))
                                    .findFirst()
                                    .map(mi -> mi.getValue())
                                    .orElse(null);
                            captured.add(new CapturedMessage(m.getData(), keyBytes));
                        }
                        event.commit();
                    }
                })
                .build();

        AsyncReader reader = topicClient.createAsyncReader(rs, hs);
        reader.init().join();
        return new TopicCapture(captured, reader, topicClient, transport, exec);
    }

    public static final class TopicCapture implements AutoCloseable {
        private final ConcurrentLinkedQueue<CapturedMessage> queue;
        private final AsyncReader reader;
        private final TopicClient topicClient;
        private final GrpcTransport transport;
        private final ScheduledExecutorService exec;

        TopicCapture(ConcurrentLinkedQueue<CapturedMessage> queue,
                     AsyncReader reader,
                     TopicClient topicClient,
                     GrpcTransport transport,
                     ScheduledExecutorService exec) {
            this.queue = queue;
            this.reader = reader;
            this.topicClient = topicClient;
            this.transport = transport;
            this.exec = exec;
        }

        public List<CapturedMessage> snapshot() {
            return new ArrayList<>(queue);
        }

        public void awaitAtLeast(int n) {
            Awaitility.await()
                    .atMost(Duration.ofSeconds(waitTimeForRecords()))
                    .pollInterval(Duration.ofMillis(500))
                    .until(() -> queue.size() >= n);
        }

        @Override
        public void close() {
            try {
                reader.shutdown().get(10, TimeUnit.SECONDS);
            }
            catch (Exception ignored) {
            }
            try {
                topicClient.close();
            }
            catch (Exception ignored) {
            }
            try {
                transport.close();
            }
            catch (Exception ignored) {
            }
            exec.shutdownNow();
        }
    }

    public static YdbAdmin openAdmin() {
        GrpcTransport transport = openTransport();
        TopicClient topicClient = TopicClient.newClient(transport).build();
        QueryClient queryClient = QueryClient.newClient(transport).build();
        return new YdbAdmin(transport, topicClient, queryClient);
    }

    public static final class YdbAdmin implements AutoCloseable {
        private final GrpcTransport transport;
        private final TopicClient topicClient;
        private final QueryClient queryClient;

        YdbAdmin(GrpcTransport transport, TopicClient topicClient, QueryClient queryClient) {
            this.transport = transport;
            this.topicClient = topicClient;
            this.queryClient = queryClient;
        }

        public boolean topicExists(String path) {
            Result<TopicDescription> res = topicClient.describeTopic(path).join();
            return res.isSuccess();
        }

        public void createTopic(String topicPath, String consumerName) {
            if (topicExists(topicPath)) {
                if (!Strings.isNullOrBlank(consumerName)) {
                    ensureConsumer(topicPath, consumerName);
                }
                return;
            }
            PartitioningSettings partitioning = PartitioningSettings.newBuilder()
                    .setMinActivePartitions(1)
                    .setMaxActivePartitions(1)
                    .build();
            SupportedCodecs codecs = SupportedCodecs.newBuilder()
                    .addCodec(Codec.RAW)
                    .addCodec(Codec.GZIP)
                    .addCodec(Codec.ZSTD)
                    .build();
            CreateTopicSettings.Builder builder = CreateTopicSettings.newBuilder()
                    .setPartitioningSettings(partitioning)
                    .setSupportedCodecs(codecs);
            if (!Strings.isNullOrBlank(consumerName)) {
                builder.addConsumer(Consumer.newBuilder().setName(consumerName).build());
            }
            tech.ydb.core.Status status = topicClient.createTopic(topicPath, builder.build()).join();
            if (status.isSuccess() || status.getCode() == tech.ydb.core.StatusCode.ALREADY_EXISTS) {
                if (!Strings.isNullOrBlank(consumerName)) {
                    ensureConsumer(topicPath, consumerName);
                }
                return;
            }
            status.expectSuccess("create topic " + topicPath);
        }

        private void ensureConsumer(String topicPath, String consumer) {
            tech.ydb.topic.settings.AlterTopicSettings alter = tech.ydb.topic.settings.AlterTopicSettings.newBuilder()
                    .addAddConsumer(Consumer.newBuilder().setName(consumer).build())
                    .build();
            tech.ydb.core.Status s = topicClient.alterTopic(topicPath, alter).join();
            if (s.isSuccess() || s.toString().toLowerCase().contains("already")) {
                return;
            }
            s.expectSuccess("alterTopic add consumer " + consumer + " on " + topicPath);
        }

        public void execYql(String yql) {
            SessionRetryContext.create(queryClient).build()
                    .supplyResult(s -> s.createQuery(yql, TxMode.NONE).execute())
                    .join().getStatus().expectSuccess(yql);
        }

        @Override
        public void close() {
            try {
                topicClient.close();
            }
            catch (Exception ignored) {
            }
            try {
                queryClient.close();
            }
            catch (Exception ignored) {
            }
            try {
                transport.close();
            }
            catch (Exception ignored) {
            }
        }
    }

    public static long jdbcRowCount(String jdbcUrl, String user, String password, String table) throws Exception {
        try (Connection c = DriverManager.getConnection(jdbcUrl, user, password);
                Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM " + table)) {
            return rs.next() ? rs.getLong(1) : 0L;
        }
    }

    private static GrpcTransport openTransport() {
        return GrpcTransport.forEndpoint(
                YdbTestResourceLifecycleManager.ENDPOINT,
                YdbTestResourceLifecycleManager.DATABASE)
                .withAuthProvider(NopAuthProvider.INSTANCE)
                .build();
    }
}
