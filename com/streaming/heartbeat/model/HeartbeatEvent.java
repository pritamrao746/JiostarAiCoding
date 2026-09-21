package com.streaming.heartbeat.model;

/**
 * Validated domain event processed internally by the component layer.
 */
public record HeartbeatEvent(
        long timestampEpochMs,
        String userId,
        String contentId,
        String platform,
        String region,
        long downloadBytes,
        boolean playbackStarted,
        double durationPlayedSec,
        String failureErrorCode,
        double rebufferDurationSec
) {}

