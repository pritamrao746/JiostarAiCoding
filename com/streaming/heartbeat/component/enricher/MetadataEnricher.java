package com.streaming.heartbeat.component.enricher;

/**
 * Interface for stream-side metadata enrichment.
 * Resolves content attributes (such as contentType) from catalog/CMS caches.
 */
public interface MetadataEnricher {

    /**
     * Resolves the contentType (e.g. "LIVE", "MOVIE", "SERIES", "VOD") for a given contentId.
     *
     * @param contentId Content / Asset identifier
     * @return Resolved contentType string
     */
    String resolveContentType(String contentId);
}

