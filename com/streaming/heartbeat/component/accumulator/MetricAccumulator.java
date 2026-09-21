package com.streaming.heartbeat.component.accumulator;

import com.streaming.heartbeat.model.AggregationKey;
import com.streaming.heartbeat.model.HeartbeatEvent;
import com.streaming.heartbeat.model.HeartbeatMetricRecord;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Set;

/**
 * Thread-safe in-memory metric accumulator for a single dimensional slice.
 */
public class MetricAccumulator {

    private long eventCount = 0;
    private long totalDownloadBytes = 0;
    private double totalDurationPlayedSec = 0.0;
    private double totalRebufferDurationSec = 0.0;
    private long playbackStartCount = 0;
    private final Set<String> uniqueUsers = new HashSet<>();

    public synchronized void accumulate(HeartbeatEvent event) {
        this.eventCount++;
        this.totalDownloadBytes += event.downloadBytes();
        this.totalDurationPlayedSec += event.durationPlayedSec();
        this.totalRebufferDurationSec += event.rebufferDurationSec();
        if (event.playbackStarted()) {
            this.playbackStartCount++;
        }
        if (event.userId() != null && !event.userId().isBlank()) {
            this.uniqueUsers.add(event.userId());
        }
    }

    /**
     * Converts accumulated state to a production POJO for the DAO layer.
     */
    public synchronized HeartbeatMetricRecord toRecord(AggregationKey key) {
        String utcString = DateTimeFormatter.ISO_INSTANT.format(
                Instant.ofEpochMilli(key.windowStartEpochMs()).atOffset(ZoneOffset.UTC)
        );

        return new HeartbeatMetricRecord(
                utcString,
                key.windowStartEpochMs(),
                key.platform(),
                key.failureErrorCode(),
                key.region(),
                key.contentType(),
                this.eventCount,
                this.totalDownloadBytes,
                Math.round(this.totalDurationPlayedSec * 100.0) / 100.0,
                Math.round(this.totalRebufferDurationSec * 100.0) / 100.0,
                this.playbackStartCount,
                this.uniqueUsers.size(),
                new HashSet<>(this.uniqueUsers)
        );
    }
}

