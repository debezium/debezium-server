/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.sns;

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
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import jakarta.enterprise.inject.Instance;

import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.debezium.engine.Header;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sns.model.PublishBatchRequest;
import software.amazon.awssdk.services.sns.model.PublishBatchRequestEntry;
import software.amazon.awssdk.services.sns.model.PublishBatchResponse;
import software.amazon.awssdk.services.sns.model.PublishBatchResultEntry;
import software.amazon.awssdk.services.sns.model.SnsException;

@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
public class SnsUnitTest {

    private SnsChangeConsumer snsChangeConsumer;
    private SnsClient spyClient;
    private AtomicInteger counter;
    private AtomicBoolean threwException;
    CapturingEvents<BatchEvent> changeEvents;
    private static final Integer NUMBER_OF_CHANGE_EVENTS = SnsChangeConsumerConfig.MAX_BATCH_SIZE;
    private static final String TEST_DEFAULT_TOPIC_ARN = "arn:aws:sns:us-east-1:000000000000:test-topic";

    @BeforeEach
    public void setup() {
        counter = new AtomicInteger(0);
        threwException = new AtomicBoolean(false);
        changeEvents = createChangeEvents(NUMBER_OF_CHANGE_EVENTS, "key", TEST_DEFAULT_TOPIC_ARN);
        spyClient = spy(SnsClient.builder().region(Region.of(SnsTestConfigSource.SNS_REGION))
                .credentialsProvider(ProfileCredentialsProvider.create("default")).build());

        Instance<SnsClient> mockInstance = mock(Instance.class);
        when(mockInstance.isResolvable()).thenReturn(true);
        when(mockInstance.get()).thenReturn(spyClient);

        snsChangeConsumer = new SnsChangeConsumer();
        snsChangeConsumer.customClient = mockInstance;
    }

    @AfterEach
    public void tearDown() {
        reset(spyClient);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static CapturingEvents<BatchEvent> createChangeEvents(int size, String key, String destination) {
        List<BatchEvent> changeEvents = new ArrayList<>();
        for (int i = 0; i < size; i++) {

            SourceRecord sourceRecord = mock(SourceRecord.class);
            when(sourceRecord.key()).thenReturn(key);
            when(sourceRecord.value()).thenReturn("value-" + i);
            when(sourceRecord.topic()).thenReturn(destination);
            when(sourceRecord.headers()).thenReturn(new ConnectHeaders().addString(key, "headerValue-" + i));

            BatchEvent event = mock(BatchEvent.class);
            when(event.key()).thenReturn(key);
            when(event.value()).thenReturn("value-" + i);
            when(event.record()).thenReturn(sourceRecord);
            Header header = mock(Header.class);
            when(header.getKey()).thenReturn(key);
            when(header.getValue()).thenReturn("headerValue-" + i);
            when(event.headers()).thenReturn(List.of(header));

            changeEvents.add(event);
        }
        return new CapturingEvents<>() {
            @Override
            public List<BatchEvent> records() {
                return changeEvents;
            }

            @Override
            public String destination() {
                return destination;
            }

            @Override
            public String source() {
                return "";
            }

            @Override
            public String engine() {
                return "";
            }
        };
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static CapturingEvents<BatchEvent> createChangeEventsWithHeaders(int size, String key, String destination, Map<String, String> headerMap) {
        List<BatchEvent> events = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            SourceRecord sourceRecord = mock(SourceRecord.class);
            when(sourceRecord.key()).thenReturn(key);
            when(sourceRecord.value()).thenReturn("value-" + i);
            when(sourceRecord.topic()).thenReturn(destination);

            ConnectHeaders connectHeaders = new ConnectHeaders();
            headerMap.forEach(connectHeaders::addString);
            when(sourceRecord.headers()).thenReturn(connectHeaders);

            BatchEvent event = mock(BatchEvent.class);
            when(event.key()).thenReturn(key);
            when(event.value()).thenReturn("value-" + i);
            when(event.record()).thenReturn(sourceRecord);

            List<Header<Object>> headers = new ArrayList<>();
            headerMap.forEach((k, v) -> {
                Header<Object> header = mock(Header.class);
                when(header.getKey()).thenReturn(k);
                when(header.getValue()).thenReturn(v);
                headers.add(header);
            });
            when(event.headers()).thenReturn(headers);

            events.add(event);
        }
        return new CapturingEvents<>() {
            @Override
            public List<BatchEvent> records() {
                return events;
            }

            @Override
            public String destination() {
                return destination;
            }

            @Override
            public String source() {
                return "";
            }

            @Override
            public String engine() {
                return "";
            }
        };
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static List<BatchEvent> createFifoChangeEvents(int size, Object rawKey, Object serializedKey,
                                                                            String destination, Map<String, String> headerMap) {
        List<BatchEvent> events = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            SourceRecord sourceRecord = mock(SourceRecord.class);
            when(sourceRecord.key()).thenReturn(rawKey);
            when(sourceRecord.value()).thenReturn("value-" + i);
            when(sourceRecord.topic()).thenReturn(destination);

            ConnectHeaders connectHeaders = new ConnectHeaders();
            headerMap.forEach(connectHeaders::addString);
            when(sourceRecord.headers()).thenReturn(connectHeaders);

            BatchEvent event = mock(BatchEvent.class);
            when(event.key()).thenReturn(serializedKey);
            when(event.value()).thenReturn("value-" + i);
            when(event.record()).thenReturn(sourceRecord);
            when(event.headers()).thenReturn(List.of());

            events.add(event);
        }
        return events;
    }

    private static PublishBatchResponse successResponse(PublishBatchRequest request) {
        List<PublishBatchResultEntry> successful = request.publishBatchRequestEntries().stream()
                .map(e -> PublishBatchResultEntry.builder().id(e.id()).messageId("msg-" + e.id()).build())
                .collect(Collectors.toList());
        return PublishBatchResponse.builder().successful(successful).failed(List.of()).build();
    }

    // 1. Test that continuous sending of SNS batch response containing errors yields exception after 5 attempts
    @Test
    public void testValidResponseWithErrorCode() throws Exception {
        // Arrange
        doAnswer(invocation -> {
            PublishBatchRequest request = invocation.getArgument(0);
            List<PublishBatchRequestEntry> entries = request.publishBatchRequestEntries();
            counter.incrementAndGet();
            List<BatchResultErrorEntry> failedEntries = entries.stream()
                    .map(entry -> BatchResultErrorEntry.builder()
                            .id(entry.id())
                            .code("InternalError")
                            .message("The request rate is too high")
                            .senderFault(false)
                            .build())
                    .collect(Collectors.toList());

            return PublishBatchResponse.builder().successful(List.of()).failed(failedEntries).build();
        }).when(spyClient).publishBatch(any(PublishBatchRequest.class));

        // Act
        try {
            snsChangeConsumer.connect();
            snsChangeConsumer.handle(changeEvents);
        }
        catch (Exception e) {
            threwException.getAndSet(true);
        }

        // Assert
        assertTrue(threwException.get());
        // DEFAULT_RETRY_COUNT is 5 times
        assertEquals(5, counter.get());
    }

    // 2. Test that continuous return of exception yields Debezium exception after 5 attempts
    @Test
    public void testExceptionWhileWritingData() throws Exception {
        // Arrange
        doAnswer(invocation -> {
            counter.incrementAndGet();
            throw SnsException.builder().message("SNS Exception").build();
        }).when(spyClient).publishBatch(any(PublishBatchRequest.class));

        // Act
        try {
            snsChangeConsumer.connect();
            snsChangeConsumer.handle(changeEvents);
        }
        catch (Exception e) {
            threwException.getAndSet(true);
        }

        // Assert
        assertTrue(threwException.get());
        // DEFAULT_RETRY_COUNT is 5 times
        assertEquals(5, counter.get());
    }

    // 3. Test that only failed records are re-sent
    @Test
    public void testResendFailedRecords() throws Exception {
        // Arrange
        AtomicBoolean firstCall = new AtomicBoolean(true);
        List<String> failedIdsFromFirstCall = new ArrayList<>();
        List<String> idsFromSecondCall = new ArrayList<>();
        doAnswer(invocation -> {
            PublishBatchRequest request = invocation.getArgument(0);
            List<PublishBatchRequestEntry> entries = request.publishBatchRequestEntries();
            counter.incrementAndGet();

            if (firstCall.get()) {
                int failedCount = 3;
                List<PublishBatchResultEntry> successful = new ArrayList<>();
                List<BatchResultErrorEntry> failed = new ArrayList<>();
                for (int i = 0; i < entries.size(); i++) {
                    if (i < failedCount) {
                        failed.add(BatchResultErrorEntry.builder()
                                .id(entries.get(i).id())
                                .code("InternalError")
                                .message("The request rate is too high")
                                .senderFault(false)
                                .build());

                        failedIdsFromFirstCall.add(entries.get(i).id());
                    }
                    else {
                        successful.add(PublishBatchResultEntry.builder()
                                .id(entries.get(i).id())
                                .messageId("msg-" + i)
                                .build());
                    }
                }
                firstCall.getAndSet(false);
                return PublishBatchResponse.builder().successful(successful).failed(failed).build();
            }
            else {
                for (PublishBatchRequestEntry entry : entries) {
                    idsFromSecondCall.add(entry.id());
                }
                return successResponse(request);
            }
        }).when(spyClient).publishBatch(any(PublishBatchRequest.class));

        // Act
        try {
            snsChangeConsumer.connect();
            snsChangeConsumer.handle(changeEvents);
        }
        catch (Exception e) {
            threwException.getAndSet(true);
        }

        // Assert
        assertFalse(threwException.get());
        assertEquals(2, counter.get());
        assertEquals(idsFromSecondCall.size(), failedIdsFromFirstCall.size());
        for (int i = 0; i < idsFromSecondCall.size(); i++) {
            assertEquals(failedIdsFromFirstCall.get(i), idsFromSecondCall.get(i));
        }
    }

    // 4. Create events for two destinations and test that they are correctly batched
    @Test
    @Disabled
    public void testBatchesAreCorrect() throws Exception {
        // Arrange
        String destinationOne = "arn:aws:sns:us-east-1:000000000000:topic-one";
        String destinationTwo = "arn:aws:sns:us-east-1:000000000000:topic-two";
        /**
         * TODO: check destination strategy

        CapturingEvents<BatchEvent> changeEvents;
        changeEvents = createChangeEvents(15, "key1", destinationOne);
        changeEvents.records().addAll(createChangeEvents(12, "key2", destinationTwo));

        AtomicInteger numRecordsDestinationOne = new AtomicInteger(0);
        AtomicInteger numRecordsDestinationTwo = new AtomicInteger(0);
        AtomicInteger numBatches = new AtomicInteger(0);

        doAnswer(invocation -> {
            PublishBatchRequest request = invocation.getArgument(0);
            String topicArn = request.topicArn();
            int size = request.publishBatchRequestEntries().size();

            if (topicArn.equals(destinationOne)) {
                numRecordsDestinationOne.addAndGet(size);
            }
            else if (topicArn.equals(destinationTwo)) {
                numRecordsDestinationTwo.addAndGet(size);
            }
            numBatches.incrementAndGet();
            return successResponse(request);
        }).when(spyClient).publishBatch(any(PublishBatchRequest.class));

        // Act
        try {
            snsChangeConsumer.connect();
            snsChangeConsumer.handle(changeEvents);
        }
        catch (Exception e) {
            threwException.getAndSet(true);
        }

        // Assert
        // No exception should be thrown
        assertFalse(threwException.get());
        // 2 destinations, 15 and 12 records each
        assertEquals(15, numRecordsDestinationOne.get());
        assertEquals(12, numRecordsDestinationTwo.get());
        // dest1: 2 batches (10+5), dest2: 2 batches (10+2)
        assertEquals(4, numBatches.get());
         */
    }

    // 5. Test that empty records are handled correctly
    @Test
    public void testEmptyRecords() throws Exception {
        // Act
        try {
            snsChangeConsumer.connect();
            snsChangeConsumer.handle(new CapturingEvents<>() {
                @Override
                public List<BatchEvent> records() {
                    return List.of();
                }

                @Override
                public String destination() {
                    return "";
                }

                @Override
                public String source() {
                    return "";
                }

                @Override
                public String engine() {
                    return "";
                }
            });
        }
        catch (Exception e) {
            threwException.getAndSet(true);
        }

        // Assert
        assertFalse(threwException.get());
    }

    // 6. Test that a batch of 25 records is correctly split into 3 batches of 10 + 10 + 5 records
    @Test
    public void testBatchSplitting() throws Exception {
        // Arrange
        CapturingEvents<BatchEvent> changeEvents = createChangeEvents(25, "key", TEST_DEFAULT_TOPIC_ARN);

        AtomicInteger numBatches = new AtomicInteger(0);
        AtomicInteger numRecordsBatchOne = new AtomicInteger(0);
        AtomicInteger numRecordsBatchTwo = new AtomicInteger(0);
        AtomicInteger numRecordsBatchThree = new AtomicInteger(0);
        AtomicBoolean firstBatch = new AtomicBoolean(true);
        AtomicBoolean secondBatch = new AtomicBoolean(false);

        doAnswer(invocation -> {
            PublishBatchRequest request = invocation.getArgument(0);
            List<PublishBatchRequestEntry> entries = request.publishBatchRequestEntries();

            for (PublishBatchRequestEntry ignored : entries) {
                if (firstBatch.get()) {
                    numRecordsBatchOne.incrementAndGet();
                }
                else if (secondBatch.get()) {
                    numRecordsBatchTwo.incrementAndGet();
                }
                else {
                    numRecordsBatchThree.incrementAndGet();
                }
            }
            numBatches.incrementAndGet();
            if (firstBatch.get()) {
                firstBatch.getAndSet(false);
                secondBatch.getAndSet(true);
            }
            else if (secondBatch.get()) {
                secondBatch.getAndSet(false);
            }
            return successResponse(request);
        }).when(spyClient).publishBatch(any(PublishBatchRequest.class));

        // Act
        try {
            snsChangeConsumer.connect();
            snsChangeConsumer.handle(changeEvents);
        }
        catch (Exception e) {
            threwException.getAndSet(true);
        }

        // Assert
        assertFalse(threwException.get());
        assertEquals(3, numBatches.get());
        assertEquals(NUMBER_OF_CHANGE_EVENTS, numRecordsBatchOne.get());
        assertEquals(NUMBER_OF_CHANGE_EVENTS, numRecordsBatchTwo.get());
        assertEquals(5, numRecordsBatchThree.get());
    }

    // 7. Test that only failed records are re-sent after successive retry
    @Test
    public void testResendFailedRecordsSuccessive() throws Exception {
        // Arrange
        AtomicBoolean firstCall = new AtomicBoolean(true);
        AtomicBoolean secondCall = new AtomicBoolean(false);
        List<String> failedIdsFromFirstCall = new ArrayList<>();
        List<String> failedIdsFromSecondCall = new ArrayList<>();
        List<String> idsFromSecondCall = new ArrayList<>();
        List<String> idsFromThirdCall = new ArrayList<>();
        doAnswer(invocation -> {
            PublishBatchRequest request = invocation.getArgument(0);
            List<PublishBatchRequestEntry> entries = request.publishBatchRequestEntries();
            counter.incrementAndGet();

            if (firstCall.get()) {
                int failedCount = 5;
                List<PublishBatchResultEntry> successful = new ArrayList<>();
                List<BatchResultErrorEntry> failed = new ArrayList<>();
                for (int i = 0; i < entries.size(); i++) {
                    if (i >= entries.size() - failedCount) {
                        failed.add(BatchResultErrorEntry.builder()
                                .id(entries.get(i).id())
                                .code("InternalError")
                                .message("The request rate is too high")
                                .senderFault(false)
                                .build());

                        failedIdsFromFirstCall.add(entries.get(i).id());
                    }
                    else {
                        successful.add(PublishBatchResultEntry.builder()
                                .id(entries.get(i).id())
                                .messageId("msg-" + i)
                                .build());
                    }
                }
                firstCall.getAndSet(false);
                secondCall.getAndSet(true);
                return PublishBatchResponse.builder().successful(successful).failed(failed).build();
            }
            if (secondCall.get()) {
                int failedCount = 2;
                List<PublishBatchResultEntry> successful = new ArrayList<>();
                List<BatchResultErrorEntry> failed = new ArrayList<>();
                for (int i = 0; i < entries.size(); i++) {
                    if (i >= entries.size() - failedCount) {
                        failed.add(BatchResultErrorEntry.builder()
                                .id(entries.get(i).id())
                                .code("InternalError")
                                .message("The request rate is too high")
                                .senderFault(false)
                                .build());
                        failedIdsFromSecondCall.add(entries.get(i).id());
                    }
                    else {
                        successful.add(PublishBatchResultEntry.builder()
                                .id(entries.get(i).id())
                                .messageId("msg-" + i)
                                .build());
                    }
                    idsFromSecondCall.add(entries.get(i).id());
                }
                secondCall.getAndSet(false);
                return PublishBatchResponse.builder().successful(successful).failed(failed).build();
            }
            else {
                for (PublishBatchRequestEntry entry : entries) {
                    idsFromThirdCall.add(entry.id());
                }
                return successResponse(request);
            }
        }).when(spyClient).publishBatch(any(PublishBatchRequest.class));

        // Act
        try {
            snsChangeConsumer.connect();
            snsChangeConsumer.handle(changeEvents);
        }
        catch (Exception e) {
            threwException.getAndSet(true);
        }

        // Assert
        assertFalse(threwException.get());
        assertEquals(3, counter.get());

        assertEquals(idsFromSecondCall.size(), failedIdsFromFirstCall.size());
        assertEquals(idsFromThirdCall.size(), failedIdsFromSecondCall.size());
        for (int i = 0; i < idsFromSecondCall.size(); i++) {
            assertEquals(failedIdsFromFirstCall.get(i), idsFromSecondCall.get(i));
        }
        for (int i = 0; i < idsFromThirdCall.size(); i++) {
            assertEquals(failedIdsFromSecondCall.get(i), idsFromThirdCall.get(i));
        }
    }

    // 8. Test that Debezium headers are forwarded as SNS MessageAttributes
    @Test
    public void testHeadersAsMessageAttributes() throws Exception {
        // Arrange
        CapturingEvents<BatchEvent> changeEvents = createChangeEventsWithHeaders(1, "key", TEST_DEFAULT_TOPIC_ARN,
                Map.of("eventType", "OrderCreated", "requestType", "command"));

        List<PublishBatchRequestEntry> capturedEntries = new ArrayList<>();

        doAnswer(invocation -> {
            PublishBatchRequest request = invocation.getArgument(0);
            capturedEntries.addAll(request.publishBatchRequestEntries());
            return successResponse(request);
        }).when(spyClient).publishBatch(any(PublishBatchRequest.class));

        // Act
        try {
            snsChangeConsumer.connect();
            snsChangeConsumer.handle(changeEvents);
        }
        catch (Exception e) {
            threwException.getAndSet(true);
        }

        // Assert
        assertFalse(threwException.get());
        assertEquals(1, capturedEntries.size());
        var attributes = capturedEntries.get(0).messageAttributes();
        assertEquals("OrderCreated", attributes.get("eventType").stringValue());
        assertEquals("command", attributes.get("requestType").stringValue());
    }

    // 9. Test that payload exceeding 256 KiB throws DebeziumException
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testPayloadSizeValidation() throws Exception {
        // Arrange
        String oversizedPayload = "x".repeat(SnsChangeConsumerConfig.MAX_SNS_MESSAGE_BYTES + 1);
        ConnectHeaders connectHeaders = new ConnectHeaders();
        SourceRecord sourceRecord = new SourceRecord(null, null, TEST_DEFAULT_TOPIC_ARN, null, null, "key", null, oversizedPayload, null, connectHeaders);

        BatchEvent event = mock(BatchEvent.class);
        when(event.key()).thenReturn("key");
        when(event.value()).thenReturn(oversizedPayload);
        when(event.record()).thenReturn(sourceRecord);
        when(event.headers()).thenReturn(List.of());

        // Act
        try {
            snsChangeConsumer.connect();
            snsChangeConsumer.handle(new CapturingEvents<BatchEvent>() {
                @Override
                public List<BatchEvent> records() {
                    return List.of(event);
                }

                @Override
                public String destination() {
                    return TEST_DEFAULT_TOPIC_ARN;
                }

                @Override
                public String source() {
                    return "";
                }

                @Override
                public String engine() {
                    return "";
                }
            });
        }
        catch (Exception e) {
            threwException.getAndSet(true);
        }

        // Assert
        assertTrue(threwException.get());
    }

    // 10. Test that FIFO MessageGroupId uses raw SourceRecord key when header is absent
    @Test
    public void testFifoMessageGroupIdUsesRawKey() throws Exception {
        // Arrange
        String fifoArn = "arn:aws:sns:us-east-1:000000000000:test-topic.fifo";
        List<BatchEvent> events = createFifoChangeEvents(1, "debezium-sns",
                "{\"schema\":{\"type\":\"struct\"},\"payload\":{\"serverName\":\"debezium-sns\"}}", fifoArn, Map.of());

        List<PublishBatchRequestEntry> capturedEntries = new ArrayList<>();

        doAnswer(invocation -> {
            PublishBatchRequest request = invocation.getArgument(0);
            counter.incrementAndGet();
            capturedEntries.addAll(request.publishBatchRequestEntries());
            return successResponse(request);
        }).when(spyClient).publishBatch(any(PublishBatchRequest.class));

        // Act
        System.setProperty("debezium.sink.sns.topic.arn", fifoArn);
        try {
            snsChangeConsumer.connect();
            snsChangeConsumer.handle(new CapturingEvents<>() {
                @Override
                public List<BatchEvent> records() {
                    return List.of();
                }

                @Override
                public String destination() {
                    return fifoArn;
                }

                @Override
                public String source() {
                    return "";
                }

                @Override
                public String engine() {
                    return "";
                }
            });
        }
        catch (Exception e) {
            threwException.getAndSet(true);
        }
        finally {
            System.clearProperty("debezium.sink.sns.topic.arn");
        }

        // Assert
        assertFalse(threwException.get());
        assertEquals(1, capturedEntries.size());
        assertEquals("debezium-sns", capturedEntries.get(0).messageGroupId());
    }

    // 11. Test that FIFO MessageGroupId falls back to default when key is null
    @Test
    public void testFifoMessageGroupIdFallsBackToDefault() throws Exception {
        // Arrange
        String fifoArn = "arn:aws:sns:us-east-1:000000000000:test-topic.fifo";
        List<BatchEvent> events = createFifoChangeEvents(1, null, null, fifoArn, Map.of());

        List<PublishBatchRequestEntry> capturedEntries = new ArrayList<>();

        doAnswer(invocation -> {
            PublishBatchRequest request = invocation.getArgument(0);
            counter.incrementAndGet();
            capturedEntries.addAll(request.publishBatchRequestEntries());
            return successResponse(request);
        }).when(spyClient).publishBatch(any(PublishBatchRequest.class));

        // Act
        System.setProperty("debezium.sink.sns.topic.arn", fifoArn);
        try {
            snsChangeConsumer.connect();
            snsChangeConsumer.handle(new CapturingEvents<BatchEvent>() {
                @Override
                public List<BatchEvent> records() {
                    return events;
                }

                @Override
                public String destination() {
                    return fifoArn;
                }

                @Override
                public String source() {
                    return "";
                }

                @Override
                public String engine() {
                    return "";
                }
            });
        }
        catch (Exception e) {
            threwException.getAndSet(true);
        }
        finally {
            System.clearProperty("debezium.sink.sns.topic.arn");
        }

        // Assert
        assertFalse(threwException.get());
        assertEquals(1, capturedEntries.size());
        assertEquals("default", capturedEntries.get(0).messageGroupId());
    }

    // 12. Test that FIFO MessageGroupId uses header value when present
    @Test
    public void testFifoMessageGroupIdUsesHeader() throws Exception {
        // Arrange
        String fifoArn = "arn:aws:sns:us-east-1:000000000000:test-topic.fifo";
        List<BatchEvent> events = createFifoChangeEvents(1, "some-key", "some-key", fifoArn,
                Map.of("aggregateId", "order-42"));

        List<PublishBatchRequestEntry> capturedEntries = new ArrayList<>();

        doAnswer(invocation -> {
            PublishBatchRequest request = invocation.getArgument(0);
            counter.incrementAndGet();
            capturedEntries.addAll(request.publishBatchRequestEntries());
            return successResponse(request);
        }).when(spyClient).publishBatch(any(PublishBatchRequest.class));

        // Act
        System.setProperty("debezium.sink.sns.topic.arn", fifoArn);
        try {
            snsChangeConsumer.connect();
            snsChangeConsumer.handle(new CapturingEvents<BatchEvent>() {
                @Override
                public List<BatchEvent> records() {
                    return events;
                }

                @Override
                public String destination() {
                    return fifoArn;
                }

                @Override
                public String source() {
                    return "";
                }

                @Override
                public String engine() {
                    return "";
                }
            });
        }
        catch (Exception e) {
            threwException.getAndSet(true);
        }
        finally {
            System.clearProperty("debezium.sink.sns.topic.arn");
        }

        // Assert
        assertFalse(threwException.get());
        assertEquals(1, capturedEntries.size());
        assertEquals("order-42", capturedEntries.get(0).messageGroupId());
    }
}
