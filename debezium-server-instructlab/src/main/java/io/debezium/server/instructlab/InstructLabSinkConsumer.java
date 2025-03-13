/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.instructlab;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine.ChangeConsumer;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.DebeziumServer;
import io.debezium.server.instructlab.transforms.AbstractAddQnaHeader;
import io.debezium.util.Strings;

/**
 * An implementation of the {@link ChangeConsumer} interface that appends change event messages
 * to the InstructLab's {@code qna.yml} file, to improve training of models.
 *
 * @author Chris Cranford
 */
@Named("instructlab")
@Dependent
public class InstructLabSinkConsumer extends BaseChangeConsumer
        implements ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(InstructLabSinkConsumer.class);

    @Inject
    DebeziumServer server;

    @PostConstruct
    void configure() {
    }

    @PreDestroy
    void close() {
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        // Read each event and build file batch data
        final Map<String, QnaFile> batchFiles = new HashMap<>();
        for (ChangeEvent<Object, Object> record : records) {
            if (record.value() != null) {
                // LOGGER.info("Received event {} = '{}'", getString(record.key()), getString(record.value()));
                getQnaMap(record).forEach((key, attributes) -> {
                    final String fileName = attributes.get(AbstractAddQnaHeader.ATTRIBUTE_FILENAME);
                    if (!Strings.isNullOrEmpty(fileName)) {
                        final String question = attributes.get(AbstractAddQnaHeader.ATTRIBUTE_QUESTION);
                        final String answer = attributes.get(AbstractAddQnaHeader.ATTRIBUTE_ANSWER);
                        if (!Strings.isNullOrEmpty(question) && !Strings.isNullOrEmpty(answer)) {
                            final QnaFile file = batchFiles.computeIfAbsent(fileName, QnaFile::new);
                            file.addSeedExample(question, answer, attributes.get(AbstractAddQnaHeader.ATTRIBUTE_CONTEXT));
                        }
                    }
                });
            }
            committer.markProcessed(record);
        }

        // Flush files
        for (QnaFile file : batchFiles.values()) {
            try {
                file.flush();
            }
            catch (IOException e) {
                throw new DebeziumException("Failed to flush file: " + file.getFileName(), e);
            }
        }

        // Mark finished
        committer.markBatchFinished();
    }

    private Map<String, Map<String, String>> getQnaMap(ChangeEvent<Object, Object> record) {
        final Map<String, Map<String, String>> headers = new HashMap<>();
        EmbeddedEngineChangeEvent<Object, Object, Object> event = (EmbeddedEngineChangeEvent<Object, Object, Object>) record;
        Headers kafkaHeaders = event.sourceRecord().headers();
        for (Header header : kafkaHeaders) {
            final String key = header.key();
            if (key.startsWith(AbstractAddQnaHeader.QNA_PREFIX)) {
                String[] keyParts = key.split("\\.", 3);
                if (keyParts.length == 3) {
                    final String prefix = keyParts[1];
                    final String attribute = keyParts[2];
                    headers.computeIfAbsent(prefix, k -> new HashMap<>()).put(attribute, String.valueOf(header.value()));
                }
            }
        }
        return headers;
    }

}
