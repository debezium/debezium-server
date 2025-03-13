/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.instructlab;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.server.DebeziumServer;
import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

/**
 * InstructLab sink integration tests.
 *
 * @author Chris Cranford
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
public class InstructLabIT {

    private static final Path FRAUD_PATH = Testing.Files.createTestingPath("fraud").toAbsolutePath();

    @Inject
    DebeziumServer server;

    {
        Testing.Files.delete(InstructLabTestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(InstructLabTestConfigSource.OFFSET_STORE_PATH);
    }

    void setupDependencies(@Observes ConnectorStartedEvent event) {
    }

    void connectorCompleted(@Observes ConnectorCompletedEvent event) throws Exception {
    }

    @BeforeEach
    public void beforeEach() throws Exception {
        Testing.Files.createTestingDirectory("fraud", true);
    }

    @Test
    public void testInstructLab() throws Exception {
        FileWatcher.create()
                .withPath(FRAUD_PATH)
                .withTimeout(Duration.ofSeconds(20))
                .withKinds(ENTRY_CREATE, ENTRY_MODIFY)
                .withHandler((kind, path) -> {
                    if (kind == ENTRY_MODIFY && path.equals(FRAUD_PATH.resolve("qna.yml"))) {
                        final String contents = Files.readString(path);
                        assertThat(contents).isEqualTo("version: 3" + System.lineSeparator() +
                                "task_description: " + path + System.lineSeparator() +
                                "created_by: Debezium" + System.lineSeparator() +
                                "seed_examples:" + System.lineSeparator() +
                                "- question: Is order 10002 potentially fraudulent?" + System.lineSeparator() +
                                "  answer: Yes, the order's quantity is greater than 1, which is the maximum allowed." + System.lineSeparator() +
                                "- question: Is order 10003 potentially fraudulent?" + System.lineSeparator() +
                                "  answer: Yes, the order's quantity is greater than 1, which is the maximum allowed." + System.lineSeparator());
                        return true;
                    }
                    return false;
                })
                .watch();
    }

    /**
     * Utility class for watching for file changes on the file system
     */
    static class FileWatcher {
        private Path path;
        private FileWatcherHandler handler;
        private Duration timeout;
        private List<WatchEvent.Kind<?>> kinds;

        private FileWatcher() {
        }

        static FileWatcher create() {
            return new FileWatcher();
        }

        FileWatcher withPath(Path path) {
            this.path = path;
            return this;
        }

        FileWatcher withHandler(FileWatcherHandler handler) {
            this.handler = handler;
            return this;
        }

        FileWatcher withTimeout(Duration timeout) {
            this.timeout = timeout;
            return this;
        }

        FileWatcher withKinds(WatchEvent.Kind<?>... kinds) {
            this.kinds = Stream.of(kinds).toList();
            return this;
        }

        void watch() throws IOException, InterruptedException {
            Objects.requireNonNull(path, "A path must be specified");
            Objects.requireNonNull(handler, "A handler must be specified");
            Objects.requireNonNull(timeout, "A timeout must be specified");

            try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
                path.register(watchService, kinds.toArray(new WatchEvent.Kind<?>[0]));
                while (true) {
                    final WatchKey key = watchService.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
                    if (key == null) {
                        throw new IOException("Failed to handle watch request before timeout");
                    }

                    for (WatchEvent<?> event : key.pollEvents()) {
                        WatchEvent.Kind<?> kind = event.kind();
                        Path filePath = (Path) event.context();
                        if (handler.onEvent(kind, path.resolve(filePath))) {
                            return;
                        }
                    }
                    key.reset();
                }
            }
        }

        @FunctionalInterface
        public interface FileWatcherHandler {
            /**
             * Handler that is called when a specific watch event is detected.
             *
             * @param kind the watch event kind type
             * @param filePath the fully qualified file path of the file that raised the event
             * @return true if the watcher should terminate, false if it should continue
             * @throws IOException if there was an error reading or working with the file
             */
            boolean onEvent(WatchEvent.Kind<?> kind, Path filePath) throws IOException;
        }
    }
}
