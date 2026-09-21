package com.streaming.heartbeat.dao;

import com.streaming.heartbeat.exception.DaoException;
import com.streaming.heartbeat.model.HeartbeatMetricRecord;

import java.util.*;

/**
 * ClickHouse implementation of HeartbeatMetricsDao.
 * Simulates ClickHouse batch inserts into downstream storage represented by an in-memory List.
 * Performs rigorous data-layer validation before persisting.
 */
public class ClickHouseHeartbeatMetricsDao implements HeartbeatMetricsDao {

    private final List<HeartbeatMetricRecord> downstreamStorage;

    public ClickHouseHeartbeatMetricsDao() {
        this.downstreamStorage = Collections.synchronizedList(new ArrayList<>());
    }

    public ClickHouseHeartbeatMetricsDao(List<HeartbeatMetricRecord> externalStorage) {
        this.downstreamStorage = Collections.synchronizedList(Objects.requireNonNull(externalStorage));
    }

    @Override
    public void persistBatch(List<HeartbeatMetricRecord> batch) throws DaoException {
        if (batch == null) {
            throw new DaoException("Cannot persist null batch to ClickHouse.");
        }
        if (batch.isEmpty()) {
            return; // No-op for empty flush
        }

        // DAO-layer validation: Verify integrity of all records in the batch
        for (HeartbeatMetricRecord record : batch) {
            validateRecord(record);
        }

        // Atomic append to downstream storage
        synchronized (downstreamStorage) {
            downstreamStorage.addAll(batch);
        }
    }

    private void validateRecord(HeartbeatMetricRecord record) throws DaoException {
        if (record == null) {
            throw new DaoException("Batch contains a null HeartbeatMetricRecord.");
        }
        if (record.windowStartEpochMs() <= 0) {
            throw new DaoException("Invalid windowStartEpochMs: " + record.windowStartEpochMs());
        }
        if (record.platform() == null || record.platform().isBlank()) {
            throw new DaoException("Record platform cannot be null or blank.");
        }
        if (record.contentType() == null || record.contentType().isBlank()) {
            throw new DaoException("Record contentType cannot be null or blank.");
        }
        if (record.totalDownloadBytes() < 0) {
            throw new DaoException("totalDownloadBytes cannot be negative: " + record.totalDownloadBytes());
        }
        if (record.eventCount() <= 0) {
            throw new DaoException("eventCount must be greater than zero: " + record.eventCount());
        }
    }

    @Override
    public long queryTotalDownloadBytes(long startEpochMs, long endEpochMs, String platform) {
        if (platform == null) return 0;

        synchronized (downstreamStorage) {
            return downstreamStorage.stream()
                    .filter(r -> r.windowStartEpochMs() >= startEpochMs && r.windowStartEpochMs() <= endEpochMs)
                    .filter(r -> platform.equalsIgnoreCase(r.platform()))
                    .mapToLong(HeartbeatMetricRecord::totalDownloadBytes)
                    .sum();
        }
    }

    @Override
    public long queryUniqueUsersWithErrorCode(long startEpochMs, long endEpochMs, String failureErrorCode) {
        if (failureErrorCode == null) return 0;

        Set<String> unionedUsers = new HashSet<>();
        synchronized (downstreamStorage) {
            downstreamStorage.stream()
                    .filter(r -> r.windowStartEpochMs() >= startEpochMs && r.windowStartEpochMs() <= endEpochMs)
                    .filter(r -> failureErrorCode.equalsIgnoreCase(r.failureErrorCode()))
                    .forEach(r -> unionedUsers.addAll(r.uniqueUsers()));
        }
        return unionedUsers.size();
    }

    @Override
    public List<HeartbeatMetricRecord> getAllPersistedRecords() {
        synchronized (downstreamStorage) {
            return new ArrayList<>(downstreamStorage);
        }
    }

    @Override
    public int getPersistedRecordCount() {
        return downstreamStorage.size();
    }
}

