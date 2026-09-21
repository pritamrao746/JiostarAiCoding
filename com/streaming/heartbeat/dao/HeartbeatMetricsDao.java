package com.streaming.heartbeat.dao;

import com.streaming.heartbeat.exception.DaoException;
import com.streaming.heartbeat.model.HeartbeatMetricRecord;

import java.util.List;

/**
 * Data Access Object interface responsible for validating and persisting
 * aggregated heartbeat telemetry, and serving time-range analytical queries.
 */
public interface HeartbeatMetricsDao {

    /**
     * Validates and persists a batch of aggregated heartbeat records to downstream ClickHouse.
     *
     * @param batch List of aggregated records.
     * @throws DaoException if batch validation fails or persistence encounters an error.
     */
    void persistBatch(List<HeartbeatMetricRecord> batch) throws DaoException;

    /**
     * Query 1: Calculates total download bytes for a specific platform within a time range.
     *
     * @param startEpochMs Window start epoch in milliseconds (inclusive)
     * @param endEpochMs   Window end epoch in milliseconds (inclusive)
     * @param platform     Target platform (e.g. "Android", "iOS")
     * @return Total bytes downloaded across all matching records
     */
    long queryTotalDownloadBytes(long startEpochMs, long endEpochMs, String platform);

    /**
     * Query 2: Calculates unique user count experiencing a specific error code within a time range.
     * Unions unique user sets across time windows to avoid double counting.
     *
     * @param startEpochMs     Window start epoch in milliseconds (inclusive)
     * @param endEpochMs       Window end epoch in milliseconds (inclusive)
     * @param failureErrorCode Target error code (e.g. "XYZ")
     * @return Distinct user count
     */
    long queryUniqueUsersWithErrorCode(long startEpochMs, long endEpochMs, String failureErrorCode);

    /**
     * Retrieve all persisted records for debugging and verification.
     */
    List<HeartbeatMetricRecord> getAllPersistedRecords();

    /**
     * Total number of persisted records in downstream storage.
     */
    int getPersistedRecordCount();
}

