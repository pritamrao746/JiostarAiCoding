package com.streaming.heartbeat.model;

import java.util.Collections;
import java.util.Set;

/**
 * Output entity POJO representing an aggregated micro-batch record to be persisted in ClickHouse.
 */
public record HeartbeatMetricRecord(
        String windowStartUtc,
        long windowStartEpochMs,
        String platform,
        String failureErrorCode,
        String region,
        String contentType,
        long eventCount,
        long totalDownloadBytes,
        double totalDurationPlayedSec,
        double totalRebufferDurationSec,
        long playbackStartCount,
        long uniqueUserCount,
        Set<String> uniqueUsers
) {
    public HeartbeatMetricRecord {
        // Defensive copy of unique users to maintain immutability
        uniqueUsers = uniqueUsers != null ? Collections.unmodifiableSet(uniqueUsers) : Collections.emptySet();
    }
}

