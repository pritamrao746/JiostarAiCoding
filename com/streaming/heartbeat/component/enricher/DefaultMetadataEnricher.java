package com.streaming.heartbeat.component.enricher;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe cache-backed implementation of MetadataEnricher.
 */
public class DefaultMetadataEnricher implements MetadataEnricher {

    private final Map<String, String> catalogCache = new ConcurrentHashMap<>();

    public DefaultMetadataEnricher() {}

    public DefaultMetadataEnricher(Map<String, String> initialCatalog) {
        if (initialCatalog != null) {
            this.catalogCache.putAll(initialCatalog);
        }
    }

    public void registerContent(String contentId, String contentType) {
        if (contentId != null && contentType != null) {
            this.catalogCache.put(contentId, contentType);
        }
    }

    @Override
    public String resolveContentType(String contentId) {
        if (contentId == null || contentId.isBlank()) {
            return "UNKNOWN";
        }

        return catalogCache.computeIfAbsent(contentId, id -> {
            if (id.startsWith("live_")) return "LIVE";
            if (id.startsWith("mov_")) return "MOVIE";
            if (id.startsWith("ep_") || id.startsWith("series_")) return "SERIES";
            return "VOD";
        });
    }
}

