package com.streaming.heartbeat.activity;

import com.streaming.heartbeat.component.HeartbeatAggregatorComponent;
import com.streaming.heartbeat.dto.HeartbeatKafkaMessageDto;
import com.streaming.heartbeat.exception.ValidationException;
import com.streaming.heartbeat.model.HeartbeatEvent;

import java.util.Objects;

/**
 * Production implementation of HeartbeatIngestionActivity.
 * Enforces contract validation on raw Kafka DTOs before delegating to the aggregator component.
 */
public class HeartbeatIngestionActivityImpl implements HeartbeatIngestionActivity {

    private final HeartbeatAggregatorComponent aggregatorComponent;

    public HeartbeatIngestionActivityImpl(HeartbeatAggregatorComponent aggregatorComponent) {
        this.aggregatorComponent = Objects.requireNonNull(aggregatorComponent, "Aggregator component must not be null");
    }

    @Override
    public void ingestHeartbeat(HeartbeatKafkaMessageDto dto) throws ValidationException {
        // 1. Structure & Null Checks
        if (dto == null) {
            throw new ValidationException("Kafka message payload is null.");
        }

        // 2. Identity & Content Validations
        if (dto.userId() == null || dto.userId().isBlank()) {
            throw new ValidationException("Validation failed: 'userId' cannot be null or empty.");
        }
        if (dto.contentId() == null || dto.contentId().isBlank()) {
            throw new ValidationException("Validation failed: 'contentId' cannot be null or empty.");
        }

        // 3. Timestamp Validation & Normalization
        if (dto.timestamp() == null || dto.timestamp() <= 0) {
            throw new ValidationException("Validation failed: 'timestamp' must be a positive epoch number.");
        }
        // Normalize seconds to milliseconds if epoch was provided in seconds (e.g. < 1e11)
        long timestampMs = dto.timestamp() < 100_000_000_000L ? dto.timestamp() * 1000L : dto.timestamp();

        // 4. Numeric Bounds & Sanity Checks
        long downloadBytes = dto.downloadBytes() != null ? dto.downloadBytes() : 0L;
        if (downloadBytes < 0) {
            throw new ValidationException("Validation failed: 'downloadBytes' cannot be negative (" + downloadBytes + ").");
        }

        double durationPlayed = dto.durationPlayed() != null ? dto.durationPlayed() : 0.0;
        if (durationPlayed < 0.0) {
            throw new ValidationException("Validation failed: 'durationPlayed' cannot be negative (" + durationPlayed + ").");
        }

        double rebufferDuration = dto.rebufferDuration() != null ? dto.rebufferDuration() : 0.0;
        if (rebufferDuration < 0.0) {
            throw new ValidationException("Validation failed: 'rebufferDuration' cannot be negative (" + rebufferDuration + ").");
        }

        // 5. Dimension Normalization
        String platform = (dto.platform() != null && !dto.platform().isBlank())
                ? dto.platform().trim()
                : "UNKNOWN";

        String region = (dto.region() != null && !dto.region().isBlank())
                ? dto.region().trim()
                : "UNKNOWN";

        String errorCode = (dto.failureErrorCodes() != null && !dto.failureErrorCodes().isBlank()
                && !dto.failureErrorCodes().equalsIgnoreCase("null")
                && !dto.failureErrorCodes().equalsIgnoreCase("none"))
                ? dto.failureErrorCodes().trim()
                : "NO_ERROR";

        boolean playbackStarted = Boolean.TRUE.equals(dto.playbackStarted());

        // 6. Map to Validated Internal Domain Event
        HeartbeatEvent domainEvent = new HeartbeatEvent(
                timestampMs,
                dto.userId().trim(),
                dto.contentId().trim(),
                platform,
                region,
                downloadBytes,
                playbackStarted,
                durationPlayed,
                errorCode,
                rebufferDuration
        );

        // 7. Pass down to Component Layer
        aggregatorComponent.aggregate(domainEvent);
    }
}

