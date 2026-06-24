/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.zerobus;

import java.util.ArrayList;
import java.util.List;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

public class ZeroBusBatchWriter {

    private final ZeroBusSinkConfig config;
    private final ZeroBusClient client;
    private final ZeroBusRecordMapper mapper;

    public ZeroBusBatchWriter(ZeroBusSinkConfig config, ZeroBusClient client, ZeroBusRecordMapper mapper) {
        this.config = config;
        this.client = client;
        this.mapper = mapper;
    }

    public List<ChangeEvent<Object, Object>> write(List<ChangeEvent<Object, Object>> records) throws InterruptedException {
        List<ChangeEvent<Object, Object>> processedRecords = new ArrayList<>();
        List<ZeroBusRecord> chunk = new ArrayList<>(config.getBatchSize());
        String currentTargetTable = null;

        for (ChangeEvent<Object, Object> record : records) {
            if (record.value() == null && ZeroBusSinkConfig.TOMBSTONE_HANDLING_DROP.equals(config.getTombstoneHandlingMode())) {
                processedRecords.add(record);
                continue;
            }

            ZeroBusRecord zeroBusRecord = mapper.map(record);
            if (currentTargetTable != null
                    && (!currentTargetTable.equals(zeroBusRecord.targetTable()) || chunk.size() >= config.getBatchSize())) {
                writeChunk(currentTargetTable, chunk);
                chunk.clear();
            }
            currentTargetTable = zeroBusRecord.targetTable();
            chunk.add(zeroBusRecord);
            processedRecords.add(record);
        }

        if (!chunk.isEmpty()) {
            writeChunk(currentTargetTable, chunk);
        }
        return processedRecords;
    }

    private void writeChunk(String targetTable, List<ZeroBusRecord> chunk) throws InterruptedException {
        int attempts = 0;
        while (true) {
            attempts++;
            try {
                ZeroBusWriteResult result = client.write(targetTable, chunk);
                if (!result.allAcknowledged(chunk.size())) {
                    throw new DebeziumException("ZeroBus partially acknowledged " + result.acknowledged().size()
                            + " result(s) for " + chunk.size() + " record(s) written to " + targetTable);
                }
                return;
            }
            catch (ZeroBusRetriableException e) {
                if (attempts >= config.getRetries()) {
                    throw new DebeziumException("Exceeded maximum number of attempts to write records to ZeroBus table " + targetTable, e);
                }
                Metronome.sleeper(config.getRetryInterval(), Clock.SYSTEM).pause();
            }
            catch (DebeziumException e) {
                throw e;
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            }
            catch (Exception e) {
                throw new DebeziumException("Failed to write records to ZeroBus table " + targetTable, e);
            }
        }
    }
}
