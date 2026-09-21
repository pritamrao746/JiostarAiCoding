package com.streaming.heartbeat.model;

/**
 * Compound dimensional key used for stream-side in-memory aggregation.
 * Automatic equals() and hashCode() ensure constant-time map lookups.
 */
public record AggregationKey(
        long windowStartEpochMs,
        String platform,
        String failureErrorCode,
        String region,
        String contentType
) {}

