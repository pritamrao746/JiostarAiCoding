package com.streaming.heartbeat;

import com.streaming.heartbeat.activity.HeartbeatIngestionActivity;
import com.streaming.heartbeat.activity.HeartbeatIngestionActivityImpl;
import com.streaming.heartbeat.component.HeartbeatAggregatorComponent;
import com.streaming.heartbeat.component.HeartbeatAggregatorComponentImpl;
import com.streaming.heartbeat.component.enricher.DefaultMetadataEnricher;
import com.streaming.heartbeat.dao.ClickHouseHeartbeatMetricsDao;
import com.streaming.heartbeat.dao.HeartbeatMetricsDao;
import com.streaming.heartbeat.dto.HeartbeatKafkaMessageDto;
import com.streaming.heartbeat.exception.ValidationException;
import com.streaming.heartbeat.model.HeartbeatMetricRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * End-to-end integration and verification suite for the 3-tier streaming pipeline:
 * [Kafka DTO] -> Activity (Validation) -> Component (Aggregator) -> DAO (ClickHouse Persistence)
 */
public class HeartbeatPipelineIntegrationTest {

    public static void main(String[] args) throws Exception {
        System.out.println("===================================================================");
        System.out.println(" RUNNING PRODUCTION PIPELINE INTEGRATION TESTS");
        System.out.println(" Architecture: Activity (Validation) -> Component (Rollup) -> DAO");
        System.out.println("===================================================================\n");

        testValidationRejections();
        testEndToEndPipelineAndAnalyticalQueries();
        testConcurrentHighThroughputIngestion();

        System.out.println("\nALL INTEGRATION TESTS COMPLETED SUCCESSFULLY!");
    }

    /**
     * Test 1: Verify that Activity layer rejects invalid DTOs with ValidationException.
     */
    private static void testValidationRejections() {
        System.out.println("--- Test 1: Testing Activity Validation Rules ---");

        HeartbeatMetricsDao dao = new ClickHouseHeartbeatMetricsDao();
        DefaultMetadataEnricher enricher = new DefaultMetadataEnricher();
        HeartbeatAggregatorComponent component = new HeartbeatAggregatorComponentImpl(
                dao, enricher, 60, 0, 1000
        );
        HeartbeatIngestionActivity activity = new HeartbeatIngestionActivityImpl(component);

        // Case A: Null DTO
        assertThrows(ValidationException.class, () -> activity.ingestHeartbeat(null), "Null DTO");

        // Case B: Blank userId
        assertThrows(ValidationException.class, () -> activity.ingestHeartbeat(
                new HeartbeatKafkaMessageDto(1790000000000L, "  ", "mov_1", "Android", "IN", 100L, false, 10.0, null, 0.0)
        ), "Blank userId");

        // Case C: Blank contentId
        assertThrows(ValidationException.class, () -> activity.ingestHeartbeat(
                new HeartbeatKafkaMessageDto(1790000000000L, "u1", "", "Android", "IN", 100L, false, 10.0, null, 0.0)
        ), "Blank contentId");

        // Case D: Negative downloadBytes
        assertThrows(ValidationException.class, () -> activity.ingestHeartbeat(
                new HeartbeatKafkaMessageDto(1790000000000L, "u1", "mov_1", "Android", "IN", -50L, false, 10.0, null, 0.0)
        ), "Negative downloadBytes");

        // Case E: Invalid/Negative timestamp
        assertThrows(ValidationException.class, () -> activity.ingestHeartbeat(
                new HeartbeatKafkaMessageDto(-100L, "u1", "mov_1", "Android", "IN", 100L, false, 10.0, null, 0.0)
        ), "Negative timestamp");

        component.close();
        System.out.println("✓ All validation rejections verified!\n");
    }

    /**
     * Test 2: Full pipeline ingestion, enrichment, batch persistence, and query execution.
     */
    private static void testEndToEndPipelineAndAnalyticalQueries() throws Exception {
        System.out.println("--- Test 2: End-to-End Ingestion, Enrichment & Target Queries ---");

        // Downstream storage
        List<HeartbeatMetricRecord> clickHouseStorage = Collections.synchronizedList(new ArrayList<>());
        HeartbeatMetricsDao dao = new ClickHouseHeartbeatMetricsDao(clickHouseStorage);

        // Metadata Catalog enrichment
        DefaultMetadataEnricher enricher = new DefaultMetadataEnricher();
        enricher.registerContent("mov_inception", "MOVIE");
        enricher.registerContent("live_worldcup", "LIVE");

        // Component & Activity setup
        HeartbeatAggregatorComponent component = new HeartbeatAggregatorComponentImpl(
                dao, enricher, 60, 0, 1000 // manual flush for test deterministic assertions
        );
        HeartbeatIngestionActivity activity = new HeartbeatIngestionActivityImpl(component);

        // Setup two 60-second windows aligned to epoch boundaries
        long window1 = (1790000000000L / 60_000L) * 60_000L;
        long window2 = window1 + 60_000L;

        // --- Window 1 Telemetry ---
        // 3 heartbeats for user_1 and user_2 on Android in IN watching Movie with error 'XYZ'
        activity.ingestHeartbeat(new HeartbeatKafkaMessageDto(
                window1, "user_1", "mov_inception", "Android", "IN", 1_000_000L, true, 60.0, "XYZ", 1.0
        ));
        activity.ingestHeartbeat(new HeartbeatKafkaMessageDto(
                window1 + 15_000L, "user_2", "mov_inception", "Android", "IN", 2_000_000L, false, 60.0, "XYZ", 0.0
        ));
        activity.ingestHeartbeat(new HeartbeatKafkaMessageDto(
                window1 + 30_000L, "user_1", "mov_inception", "Android", "IN", 1_500_000L, false, 30.0, "XYZ", 0.5
        ));

        // 1 heartbeat for user_3 on iOS watching Movie with error 'XYZ'
        activity.ingestHeartbeat(new HeartbeatKafkaMessageDto(
                window1 + 20_000L, "user_3", "mov_inception", "iOS", "IN", 3_000_000L, true, 60.0, "XYZ", 0.0
        ));

        // 1 heartbeat for user_4 on Android watching Live stream with NO_ERROR in US
        activity.ingestHeartbeat(new HeartbeatKafkaMessageDto(
                window1 + 25_000L, "user_4", "live_worldcup", "Android", "US", 5_000_000L, true, 60.0, null, 0.0
        ));

        // --- Window 2 Telemetry ---
        // user_1 (seen earlier) and user_5 (new) encounter error 'XYZ' on Android
        activity.ingestHeartbeat(new HeartbeatKafkaMessageDto(
                window2, "user_1", "mov_inception", "Android", "IN", 1_200_000L, false, 60.0, "XYZ", 0.0
        ));
        activity.ingestHeartbeat(new HeartbeatKafkaMessageDto(
                window2 + 10_000L, "user_5", "mov_inception", "Android", "IN", 2_800_000L, true, 60.0, "XYZ", 1.5
        ));

        // Flush component buffer to DAO
        int flushedCount = component.flush();
        System.out.println("Flushed " + flushedCount + " aggregated records into ClickHouse DAO.");

        // Verify persisted records
        List<HeartbeatMetricRecord> records = dao.getAllPersistedRecords();
        System.out.println("\nPersisted Records in DAO:");
        for (HeartbeatMetricRecord r : records) {
            System.out.printf("  [%s] Platform: %-7s | Error: %-8s | ContentType: %-6s | Events: %d | Bytes: %,d | Users: %d%n",
                    r.windowStartUtc(), r.platform(), r.failureErrorCode(), r.contentType(),
                    r.eventCount(), r.totalDownloadBytes(), r.uniqueUserCount());
        }

        // -----------------------------------------------------------------
        // QUERY 1: For time range [window1, window2], how many bytes downloaded from Android?
        // -----------------------------------------------------------------
        long androidBytes = dao.queryTotalDownloadBytes(window1, window2, "Android");
        System.out.println("\n-----------------------------------------------------------------");
        System.out.printf("QUERY 1 Result (Android Total Bytes): %,d bytes%n", androidBytes);
        // Window 1 Android: 1,000,000 + 2,000,000 + 1,500,000 + 5,000,000 = 9,500,000
        // Window 2 Android: 1,200,000 + 2,800,000 = 4,000,000
        // Total Expected = 13,500,000
        if (androidBytes != 13_500_000L) {
            throw new AssertionError("Expected 13,500,000 bytes, got: " + androidBytes);
        }
        System.out.println("✓ Query 1 Verified!");

        // -----------------------------------------------------------------
        // QUERY 2: How many unique users are facing 'XYZ' error code in time range [window1, window2]?
        // -----------------------------------------------------------------
        long uniqueUsersWithXYZ = dao.queryUniqueUsersWithErrorCode(window1, window2, "XYZ");
        System.out.printf("QUERY 2 Result (Unique Users Facing 'XYZ'): %d unique users%n", uniqueUsersWithXYZ);
        // Window 1 XYZ: user_1, user_2 (Android), user_3 (iOS)
        // Window 2 XYZ: user_1, user_5 (Android)
        // Distinct set across windows: {user_1, user_2, user_3, user_5} = 4 users
        if (uniqueUsersWithXYZ != 4) {
            throw new AssertionError("Expected 4 unique users, got: " + uniqueUsersWithXYZ);
        }
        System.out.println("✓ Query 2 Verified!");
        System.out.println("-----------------------------------------------------------------\n");

        component.close();
    }

    /**
     * Test 3: Concurrent high-throughput stress test across multiple simulated Kafka consumer threads.
     */
    private static void testConcurrentHighThroughputIngestion() throws Exception {
        System.out.println("--- Test 3: Concurrent Ingestion & Background Auto-Flush ---");

        HeartbeatMetricsDao dao = new ClickHouseHeartbeatMetricsDao();
        DefaultMetadataEnricher enricher = new DefaultMetadataEnricher();

        // 60-second window, flush every 1 second or every 50 distinct keys
        try (HeartbeatAggregatorComponent component = new HeartbeatAggregatorComponentImpl(
                dao, enricher, 60, 1, 50
        )) {
            HeartbeatIngestionActivity activity = new HeartbeatIngestionActivityImpl(component);

            int numThreads = 8;
            int messagesPerThread = 200;
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            CountDownLatch latch = new CountDownLatch(numThreads);

            long baseTime = (1790000000000L / 60_000L) * 60_000L;

            for (int t = 0; t < numThreads; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    try {
                        for (int i = 0; i < messagesPerThread; i++) {
                            long ts = baseTime + ((i % 3) * 60_000L); // 3 different window buckets
                            activity.ingestHeartbeat(new HeartbeatKafkaMessageDto(
                                    ts,
                                    "user_" + threadId + "_" + (i % 20),
                                    "mov_" + (i % 4),
                                    (threadId % 2 == 0) ? "Android" : "iOS",
                                    "IN",
                                    500L,
                                    (i == 0),
                                    60.0,
                                    (i % 5 == 0) ? "XYZ" : null,
                                    0.0
                            ));
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                });
            }

            latch.await();
            executor.shutdown();

            // Allow background flusher to finish and trigger close
            component.close();

            int totalRecords = dao.getPersistedRecordCount();
            long totalBytes = dao.getAllPersistedRecords().stream()
                    .mapToLong(HeartbeatMetricRecord::totalDownloadBytes)
                    .sum();
            long totalEvents = dao.getAllPersistedRecords().stream()
                    .mapToLong(HeartbeatMetricRecord::eventCount)
                    .sum();

            long expectedTotalEvents = (long) numThreads * messagesPerThread;
            long expectedTotalBytes = expectedTotalEvents * 500L;

            System.out.printf("Total Ingested Events: %,d (Expected: %,d)%n", totalEvents, expectedTotalEvents);
            System.out.printf("Total Download Bytes: %,d (Expected: %,d)%n", totalBytes, expectedTotalBytes);
            System.out.printf("Total Aggregated Rolled-up Rows: %d%n", totalRecords);

            if (totalEvents != expectedTotalEvents) {
                throw new AssertionError("Mismatch in total events!");
            }
            if (totalBytes != expectedTotalBytes) {
                throw new AssertionError("Mismatch in total bytes!");
            }

            System.out.println("✓ Concurrent Ingestion Stress Test Passed without lost updates!");
        }
    }

    private static void assertThrows(Class<? extends Throwable> expected, RunnableAction action, String scenario) {
        try {
            action.run();
            throw new AssertionError("Expected " + expected.getSimpleName() + " for scenario: " + scenario);
        } catch (Throwable t) {
            if (expected.isInstance(t)) {
                System.out.println("  ✓ Correctly rejected: " + scenario + " -> " + t.getMessage());
            } else {
                throw new AssertionError("Unexpected exception for scenario: " + scenario, t);
            }
        }
    }

    @FunctionalInterface
    private interface RunnableAction {
        void run() throws Throwable;
    }
}

