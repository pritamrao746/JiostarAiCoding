package com.streaming.heartbeat.activity;

import com.streaming.heartbeat.dto.HeartbeatKafkaMessageDto;
import com.streaming.heartbeat.exception.ValidationException;

/**
 * Top-level Activity interface for Kafka heartbeat message ingestion.
 * Performs schema/semantic validation and forwards valid messages to the aggregator component.
 */
public interface HeartbeatIngestionActivity {

    /**
     * Ingests, validates, and forwards a raw Kafka message DTO.
     *
     * @param dto Raw Kafka message payload
     * @throws ValidationException if the message fails validation constraints
     */
    void ingestHeartbeat(HeartbeatKafkaMessageDto dto) throws ValidationException;
}

