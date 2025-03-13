/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.instructlab;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.util.Testing;

/**
 * Unit tests for {@link QnaFile}.
 *
 * @author Chris Cranford
 */
public class QnaFileTest {

    private static final String QNA_YAML = Testing.Files.createTestingPath("test.yml").toAbsolutePath().toString();

    @BeforeAll
    public static void beforeAll() {
        // Makes sure the "target/data" directory is created
        Testing.Files.createTestingDirectory("");
    }

    @BeforeEach
    public void beforeEach() {
        // Deletes any remaining YAML files after each test
        Testing.Files.delete(QNA_YAML);
    }

    @Test
    public void testCreateEmptyQnaFile() throws Exception {
        final QnaFile file = new QnaFile(QNA_YAML);
        file.flush();

        final String content = Files.readString(Path.of(QNA_YAML));
        assertThat(content).isEqualTo("version: 3" + System.lineSeparator() +
                "task_description: " + QNA_YAML + System.lineSeparator() +
                "created_by: Debezium" + System.lineSeparator());
    }

    @Test
    public void testCreateQnaFileWithQuestionAnswer() throws Exception {
        final QnaFile file = new QnaFile(QNA_YAML);
        file.addSeedExample("What is the answer to the universe?", "The answer is 42", null);
        file.flush();

        final String content = Files.readString(Path.of(QNA_YAML));
        assertThat(content).isEqualTo("version: 3" + System.lineSeparator() +
                "task_description: " + QNA_YAML + System.lineSeparator() +
                "created_by: Debezium" + System.lineSeparator() +
                "seed_examples:" + System.lineSeparator() +
                "- question: What is the answer to the universe?" + System.lineSeparator() +
                "  answer: The answer is 42" + System.lineSeparator());
    }

    @Test
    public void testCreateQnaFileWithQuestionAnswerAndContext() throws Exception {
        final QnaFile file = new QnaFile(QNA_YAML);
        file.addSeedExample("What is the answer to the universe?", "The answer is 42", "Source Hitchhiker's Guide to the Galaxy");
        file.flush();

        final String content = Files.readString(Path.of(QNA_YAML));
        assertThat(content).isEqualTo("version: 3" + System.lineSeparator() +
                "task_description: " + QNA_YAML + System.lineSeparator() +
                "created_by: Debezium" + System.lineSeparator() +
                "seed_examples:" + System.lineSeparator() +
                "- question: What is the answer to the universe?" + System.lineSeparator() +
                "  answer: The answer is 42" + System.lineSeparator() +
                "  context: Source Hitchhiker's Guide to the Galaxy" + System.lineSeparator());
    }

    @Test
    public void testAppendQnaFileQuestionAnswer() throws Exception {
        Files.writeString(Path.of(QNA_YAML),
                "version: 3" + System.lineSeparator() +
                        "task_description: Some desc" + System.lineSeparator() +
                        "created_by: ccranfor" + System.lineSeparator() +
                        "seed_examples:" + System.lineSeparator() +
                        "- question: What is 2+2?" + System.lineSeparator() +
                        "  answer: 4" + System.lineSeparator());

        final QnaFile file = new QnaFile(QNA_YAML);
        file.addSeedExample("What is the answer to the universe?", "The answer is 42", null);
        file.flush();

        final String content = Files.readString(Path.of(QNA_YAML));
        assertThat(content).isEqualTo("version: 3" + System.lineSeparator() +
                "task_description: Some desc" + System.lineSeparator() +
                "created_by: ccranfor" + System.lineSeparator() +
                "seed_examples:" + System.lineSeparator() +
                "- question: What is 2+2?" + System.lineSeparator() +
                "  answer: 4" + System.lineSeparator() +
                "- question: What is the answer to the universe?" + System.lineSeparator() +
                "  answer: The answer is 42" + System.lineSeparator());
    }

    @Test
    public void testAppendQnaFileQuestionAnswerContext() throws Exception {
        Files.writeString(Path.of(QNA_YAML),
                "version: 3" + System.lineSeparator() +
                        "task_description: Some desc" + System.lineSeparator() +
                        "created_by: ccranfor" + System.lineSeparator() +
                        "seed_examples:" + System.lineSeparator() +
                        "- question: What is 2+2?" + System.lineSeparator() +
                        "  answer: 4" + System.lineSeparator());

        final QnaFile file = new QnaFile(QNA_YAML);
        file.addSeedExample("What is the answer to the universe?", "The answer is 42", "Source Hitchhiker's Guide to the Galaxy");
        file.flush();

        final String content = Files.readString(Path.of(QNA_YAML));
        assertThat(content).isEqualTo("version: 3" + System.lineSeparator() +
                "task_description: Some desc" + System.lineSeparator() +
                "created_by: ccranfor" + System.lineSeparator() +
                "seed_examples:" + System.lineSeparator() +
                "- question: What is 2+2?" + System.lineSeparator() +
                "  answer: 4" + System.lineSeparator() +
                "- question: What is the answer to the universe?" + System.lineSeparator() +
                "  answer: The answer is 42" + System.lineSeparator() +
                "  context: Source Hitchhiker's Guide to the Galaxy" + System.lineSeparator());
    }

}
