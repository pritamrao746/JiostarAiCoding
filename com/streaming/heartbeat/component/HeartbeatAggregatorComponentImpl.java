package com.streaming.heartbeat.component;

import com.streaming.heartbeat.component.accumulator.MetricAccumulator;
import com.streaming.heartbeat.component.enricher.DefaultMetadataEnricher;
import com.streaming.heartbeat.component.enricher.MetadataEnricher;
import com.streaming.heartbeat.dao.HeartbeatMetricsDao;
import com.streaming.heartbeat.model.AggregationKey;
import com.streaming.heartbeat.model.HeartbeatEvent;
import com.streaming.heartbeat.model.HeartbeatMetricRecord;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Production implementation of HeartbeatAggregatorComponent.
 * Handles metadata enrichment, stateful micro-batch accumulation, and scheduled / threshold-based
 * flushes to the DAO layer with non-blocking atomic buffer swapping.
 */
public class HeartbeatAggregatorComponentImpl implements HeartbeatAggregatorComponent {

    private final HeartbeatMetricsDao dao;
    private final MetadataEnricher metadataEnricher;
    private final long windowSizeMs;
    private final int maxKeysBeforeFlush;
    private final ScheduledExecutorService scheduler;

    // High-throughput concurrency: ReadLock for ingestion, WriteLock for atomic buffer swap
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private Map<AggregationKey, MetricAccumulator> activeBuffer = new HashMap<>();

    public HeartbeatAggregatorComponentImpl(
            HeartbeatMetricsDao dao,
            MetadataEnricher metadataEnricher,
            long windowSizeSeconds,
            long flushIntervalSeconds,
            int maxKeysBeforeFlush
    ) {
        this.dao = Objects.requireNonNull(dao, "HeartbeatMetricsDao must not be null");
        this.metadataEnricher = metadataEnricher != null ? metadataEnricher : new DefaultMetadataEnricher();
        this.windowSizeMs = windowSizeSeconds * 1000L;
        this.maxKeysBeforeFlush = maxKeysBeforeFlush;

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "heartbeat-aggregator-flusher");
            t.setDaemon(true);
            return t;
        });

        if (flushIntervalSeconds > 0) {
            this.scheduler.scheduleAtFixedRate(
                    this::flush,
                    flushIntervalSeconds,
                    flushIntervalSeconds,
                    TimeUnit.SECONDS
            );
        }
    }

    @Override
    public void aggregate(HeartbeatEvent event) {
        if (event == null) return;

        // 1. Metadata Enrichment (contentId -> contentType)
        String contentType = metadataEnricher.resolveContentType(event.contentId());

        // 2. Align event timestamp to window bucket
        long windowStart = (event.timestampEpochMs() / this.windowSizeMs) * this.windowSizeMs;

        // 3. Construct compound aggregation key
        AggregationKey key = new AggregationKey(
                windowStart,
                event.platform(),
                event.failureErrorCode(),
                event.region(),
                contentType
        );

        // 4. Thread-safe accumulation under ReadLock
        boolean shouldTriggerFlush = false;
        rwLock.readLock().lock();
        try {
            MetricAccumulator accumulator;
            synchronized (activeBuffer) {
                accumulator = activeBuffer.computeIfAbsent(key, k -> new MetricAccumulator());
                if (activeBuffer.size() >= maxKeysBeforeFlush) {
                    shouldTriggerFlush = true;
                }
            }
            accumulator.accumulate(event);
        } finally {
            rwLock.readLock().unlock();
        }

        // 5. Trigger flush if key threshold reached
        if (shouldTriggerFlush) {
            flush();
        }
    }

    @Override
    public int flush() {
        Map<AggregationKey, MetricAccumulator> bufferToFlush;

        // Hold WriteLock only for pointer swap (microseconds)
        rwLock.writeLock().lock();
        try {
            if (activeBuffer.isEmpty()) {
                return 0;
            }
            bufferToFlush = activeBuffer;
            activeBuffer = new HashMap<>();
        } finally {
            rwLock.writeLock().unlock();
        }

        // Convert accumulated state into POJO records outside the lock
        List<HeartbeatMetricRecord> batch = new ArrayList<>(bufferToFlush.size());
        for (Map.Entry<AggregationKey, MetricAccumulator> entry : bufferToFlush.entrySet()) {
            batch.add(entry.getValue().toRecord(entry.getKey()));
        }

        // Persist batch through DAO layer
        dao.persistBatch(batch);
        return batch.size();
    }

    @Override
    public int getActiveKeyCount() {
        rwLock.readLock().lock();
        try {
            synchronized (activeBuffer) {
                return activeBuffer.size();
            }
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public void close() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(2, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        flush();
    }
}

