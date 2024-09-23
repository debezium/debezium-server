/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.kinesis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import jakarta.enterprise.inject.Instance;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.engine.Header;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
public class KinesisUnitTest {

    private KinesisChangeConsumer kinesisChangeConsumer;
    private KinesisClient spyClient;
    private AtomicInteger counter;
    private AtomicBoolean threwException;
    List<ChangeEvent<Object, Object>> changeEvents;
    RecordCommitter<ChangeEvent<Object, Object>> committer;

    @BeforeEach
    public void setup() {
        counter = new AtomicInteger(0);
        threwException = new AtomicBoolean(false);
        changeEvents = createChangeEvents(500, "destination");
        committer = RecordCommitter();
        spyClient = spy(KinesisClient.builder().region(Region.of(KinesisTestConfigSource.KINESIS_REGION))
                .credentialsProvider(ProfileCredentialsProvider.create("default")).build());

        Instance<KinesisClient> mockInstance = mock(Instance.class);
        when(mockInstance.isResolvable()).thenReturn(true);
        when(mockInstance.get()).thenReturn(spyClient);

        kinesisChangeConsumer = new KinesisChangeConsumer();
        kinesisChangeConsumer.customClient = mockInstance;
        kinesisChangeConsumer.batchSize = 500;
        kinesisChangeConsumer.RETRIES = 5;
    }

    @AfterEach
    public void tearDown() {
        reset(spyClient);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static List<ChangeEvent<Object, Object>> createChangeEvents(int size, String destination) {
        List<ChangeEvent<Object, Object>> changeEvents = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            ChangeEvent<Object, Object> result = mock(ChangeEvent.class);
            when(result.key()).thenReturn("key");
            when(result.value()).thenReturn(Integer.toString(i));
            when(result.destination()).thenReturn(destination);
            Header header = mock(Header.class);
            when(header.getKey()).thenReturn("h1Key");
            when(header.getValue()).thenReturn("h1Value");
            when(result.headers()).thenReturn(List.of(header));
            changeEvents.add(result);
        }
        return changeEvents;
    }

    @SuppressWarnings({ "unchecked" })
    private static RecordCommitter<ChangeEvent<Object, Object>> RecordCommitter() {
        RecordCommitter<ChangeEvent<Object, Object>> result = mock(RecordCommitter.class);
        return result;
    }

    // 1. Test that continous sending of Kinesis response containing error yields exception after 5 attempts
    @Test
    public void testValidResponseWithErrorCode() throws Exception {
        // Arrange
        doAnswer(invocation -> {
            PutRecordsRequest request = invocation.getArgument(0);
            List<PutRecordsRequestEntry> records = request.records();
            counter.incrementAndGet();
            List<PutRecordsResultEntry> failedEntries = records.stream().map(record -> PutRecordsResultEntry.builder().errorCode("ProvisionedThroughputExceededException")
                    .errorMessage("The request rate for the stream is too high").build()).collect(Collectors.toList());

            return PutRecordsResponse.builder().failedRecordCount(records.size()).records(failedEntries).build();
        }).when(spyClient).putRecords(any(PutRecordsRequest.class));

        // Act
        try {
            kinesisChangeConsumer.connect();
            kinesisChangeConsumer.handleBatch(changeEvents, RecordCommitter());
        }
        catch (Exception e) {
            threwException.getAndSet(true);
        }

        // Assert
        assertTrue(threwException.get());
        // DEFAULT_RETRIES is 5 times
        assertEquals(5, counter.get());
    }

    // 2. Test that continous return of exception yields Debezium exception after 5 attempts
    @Test
    public void testExceptionWhileWritingData() throws Exception {
        // Arrange
        doAnswer(invocation -> {
            counter.incrementAndGet();
            throw KinesisException.builder().message("Kinesis Exception").build();
        }).when(spyClient).putRecords(any(PutRecordsRequest.class));

        // Act
        try {
            kinesisChangeConsumer.connect();
            kinesisChangeConsumer.handleBatch(changeEvents, committer);
        }
        catch (Exception e) {
            threwException.getAndSet(true);
        }

        // Assert
        assertTrue(threwException.get());
        // DEFAULT_RETRIES is 5 times
        assertEquals(5, counter.get());
    }

    // 3. Test that only failed records are re-sent
    @Test
    public void testResendFailedRecords() throws Exception {
        // Arrange
        AtomicBoolean firstCall = new AtomicBoolean(true);
        List<PutRecordsRequestEntry> failedRecordsFromFirstCall = new ArrayList<>();
        List<PutRecordsRequestEntry> recordsFromSecondCall = new ArrayList<>();
        doAnswer(invocation -> {
            List<PutRecordsResultEntry> response = new ArrayList<>();
            PutRecordsRequest request = invocation.getArgument(0);
            List<PutRecordsRequestEntry> records = request.records();
            counter.incrementAndGet();

            if (firstCall.get()) {
                int failedEntries = 100;
                for (int i = 0; i < records.size(); i++) {
                    PutRecordsResultEntry recordResult;
                    if (i < failedEntries) {
                        recordResult = PutRecordsResultEntry.builder().errorCode("ProvisionedThroughputExceededException")
                                .errorMessage("The request rate for the stream is too high").build();

                        failedRecordsFromFirstCall.add(records.get(i));
                    }
                    else {
                        recordResult = PutRecordsResultEntry.builder().shardId("shardId").sequenceNumber("sequenceNumber").build();
                    }
                    response.add(recordResult);
                }
                firstCall.getAndSet(false);
                return PutRecordsResponse.builder().failedRecordCount(failedEntries).records(response).build();
            }
            else {
                for (PutRecordsRequestEntry record : records) {
                    recordsFromSecondCall.add(record);
                    PutRecordsResultEntry recordResult = PutRecordsResultEntry.builder().shardId("shardId").sequenceNumber("sequenceNumber").build();
                    response.add(recordResult);
                }
                return PutRecordsResponse.builder().failedRecordCount(0).records(response).build();
            }
        }).when(spyClient).putRecords(any(PutRecordsRequest.class));

        // Act
        try {
            kinesisChangeConsumer.connect();
            kinesisChangeConsumer.handleBatch(changeEvents, committer);
        }
        catch (Exception e) {
            threwException.getAndSet(true);
        }

        // Assert
        assertFalse(threwException.get());
        assertEquals(2, counter.get());
        assertEquals(recordsFromSecondCall.size(), failedRecordsFromFirstCall.size());
        for (int i = 0; i < recordsFromSecondCall.size(); i++) {
            assertEquals(failedRecordsFromFirstCall.get(i).data(), recordsFromSecondCall.get(i).data());
        }
    }

    @Test
    public void testBatchesAreCorrect() throws Exception {
        // Arrange
        AtomicInteger numRecordsDestinationOne = new AtomicInteger(0);
        AtomicInteger numRrecordsDestinationTwo = new AtomicInteger(0);

        doAnswer(invocation -> {
            List<PutRecordsResultEntry> response = new ArrayList<>();
            PutRecordsRequest request = invocation.getArgument(0);
            List<PutRecordsRequestEntry> records = request.records();
            counter.incrementAndGet();

            else {
                for (PutRecordsRequestEntry record : records) {
                    recordsFromSecondCall.add(record);
                    PutRecordsResultEntry recordResult = PutRecordsResultEntry.builder().shardId("shardId").sequenceNumber("sequenceNumber").build();
                    response.add(recordResult);
                }
                return PutRecordsResponse.builder().failedRecordCount(0).records(response).build();
            }
        }).when(spyClient).putRecords(any(PutRecordsRequest.class));

        // Act
        try {
            kinesisChangeConsumer.connect();
            kinesisChangeConsumer.handleBatch(changeEvents, committer);
        }
        catch (Exception e) {
            threwException.getAndSet(true);
        }

        // Assert
        assertTrue(threwException.get());
        // DEFAULT_RETRIES is 5 times
        assertEquals(5, counter.get());
}
