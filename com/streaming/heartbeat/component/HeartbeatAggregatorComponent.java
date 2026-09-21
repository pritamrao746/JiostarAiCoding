package com.streaming.heartbeat.component;

import com.streaming.heartbeat.model.HeartbeatEvent;

/**
 * Component interface responsible for in-memory stream-side accumulation,
 * metadata enrichment, and managing batch flushes to the DAO layer.
 */
public interface HeartbeatAggregatorComponent extends AutoCloseable {

    /**
     * Enriches and aggregates an incoming validated domain event.
     *
     * @param event Validated HeartbeatEvent
     */
    void aggregate(HeartbeatEvent event);

    /**
     * Flushes currently accumulated metrics downstream to the DAO.
     *
     * @return Number of aggregated records persisted
     */
    int flush();

    /**
     * Number of active distinct dimensional keys currently accumulated in memory.
     */
    int getActiveKeyCount();

    @Override
    void close();
}

