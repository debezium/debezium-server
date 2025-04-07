/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.instructlab;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.awaitility.Awaitility;

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

    {
        Testing.Files.delete(InstructLabTestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(InstructLabTestConfigSource.OFFSET_STORE_PATH);
    }

    @BeforeEach
    public void beforeEach() throws Exception {
        Testing.Files.delete(Testing.Files.createTestingPath("taxonomies"));
    }

    @Test
    public void testInstructLab() throws Exception {
        final Path t1Path = Testing.Files.createTestingPath("taxonomies/t1/a/b/c/qna.yml");
        final Path t2Path = Testing.Files.createTestingPath("taxonomies/t2/x/y/z/qna.yml");

        Awaitility.await()
                .atMost(Duration.ofMinutes(2))
                .untilAsserted(() -> {
                    try {
                        final String t1Contents = Files.readString(t1Path);
                        final String t2Contents = Files.readString(t2Path);

                        // Check that taxonomy t1 was created
                        assertThat(t1Contents).isEqualTo(ofLines(
                                "version: 3",
                                "task_description: " + t1Path.toAbsolutePath().toString(),
                                "created_by: Debezium",
                                "seed_examples:",
                                "- question: '1001'",
                                "  answer: '102'",
                                "  context: '1'",
                                "- question: '1002'",
                                "  answer: '105'",
                                "  context: '2'",
                                "- question: '1002'",
                                "  answer: '106'",
                                "  context: '2'",
                                "- question: '1003'",
                                "  answer: '107'",
                                "  context: '1'"));

                        assertThat(t2Contents).isEqualTo(ofLines(
                                "version: 3",
                                "task_description: " + t2Path.toAbsolutePath().toString(),
                                "created_by: Debezium",
                                "seed_examples:",
                                "- question: '1001'",
                                "  answer: '102'",
                                "- question: '1002'",
                                "  answer: '105'",
                                "- question: '1002'",
                                "  answer: '106'",
                                "- question: '1003'",
                                "  answer: '107'"));
                    }
                    catch (IOException e) {
                        throw new AssertionError(e);
                    }
                });
    }

    private static String ofLines(String... lines) {
        return Stream.of(lines).collect(Collectors.joining(System.lineSeparator())) + System.lineSeparator();
    }
}
