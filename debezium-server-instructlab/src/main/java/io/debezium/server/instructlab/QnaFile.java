/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.instructlab;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;

import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import io.debezium.util.Strings;

/**
 * Provides functionality for working with an InstructLab {@code qna.yml} file.
 *
 * @author Chris Cranford
 */
public class QnaFile {

    private final String fileName;
    private final Queue<Example> queue;

    public QnaFile(String fileName) {
        this.fileName = fileName;
        this.queue = new LinkedBlockingDeque<>();
    }

    /**
     * Get the filesystem file name
     *
     * @return the file name
     */
    public String getFileName() {
        return fileName;
    }

    /**
     * Add a specific seed example question/answer tuple.
     *
     * @param question the example question, should not be {@code null} or empty
     * @param answer the example answer, should not be {@code null} or empty
     */
    public void addSeedExample(String question, String answer, String context) {
        this.queue.add(new Example(question, answer, context));
    }

    /**
     * Flushes any queued examples to the filesystem.
     */
    public void flush() throws IOException {
        final DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        options.setPrettyFlow(true);

        final Yaml yaml = new Yaml(options);
        final File file = new File(fileName);

        final HashMap<String, Object> data;
        if (file.exists()) {
            data = yaml.load(new FileInputStream(file));
        }
        else {
            data = new LinkedHashMap<>();
            data.put("version", 3);
            data.put("task_description", fileName);
            data.put("created_by", "Debezium");
        }

        if (!queue.isEmpty()) {
            final List<Object> examples = (List<Object>) data.computeIfAbsent("seed_examples", k -> new ArrayList<>());

            queue.forEach(entry -> {
                Map<String, Object> map = new LinkedHashMap<>();
                map.put("question", entry.getQuestion());
                map.put("answer", entry.getAnswer());

                if (!Strings.isNullOrEmpty(entry.getContext())) {
                    map.put("context", entry.getContext());
                }

                examples.add(map);
            });
        }

        // Make sure parent path exists
        if (!file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }

        try (FileWriter writer = new FileWriter(file)) {
            yaml.dump(data, writer);
        }
    }

    static class Example {
        final String question;
        final String answer;
        final String context;

        Example(String question, String answer, String context) {
            this.question = question;
            this.answer = answer;
            this.context = context;
        }

        public String getQuestion() {
            return question;
        }

        public String getAnswer() {
            return answer;
        }

        public String getContext() {
            return context;
        }
    }
}
