package com.streaming.heartbeat.dto;

/**
 * Raw Data Transfer Object representing the deserialized Kafka message payload.
 * Matches incoming JSON telemetry from streaming video clients.
 */
public record HeartbeatKafkaMessageDto(
        Long timestamp,                // Epoch milliseconds or seconds
        String userId,
        String contentId,
        String platform,               // Android, iOS, Web, SmartTV, etc.
        String region,                 // IN, US, GB, etc.
        Long downloadBytes,
        Boolean playbackStarted,
        Double durationPlayed,         // In seconds
        String failureErrorCodes,      // e.g. "XYZ", or null/empty
        Double rebufferDuration        // In seconds
) {}

