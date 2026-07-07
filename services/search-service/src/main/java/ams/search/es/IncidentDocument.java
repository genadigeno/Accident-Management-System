package ams.search.es;

/**
 * The searchable form of a reported incident, as stored in the {@code ams-incidents}
 * Elasticsearch index. {@code cacheId} is the document id, so re-indexing the same report is an
 * idempotent overwrite.
 */
public record IncidentDocument(
        String cacheId,
        String type,
        String description,
        String address,
        GeoPoint location,
        long reportedAt) {

    /** Elasticsearch {@code geo_point} shape: {@code {"lat":..,"lon":..}}. */
    public record GeoPoint(double lat, double lon) {}
}
