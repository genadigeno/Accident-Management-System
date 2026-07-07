package ams.search.service;

import ams.search.es.ElasticsearchGateway;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * Read side: builds Elasticsearch query bodies for full-text and geo/time searches over the
 * indexed incident history and shapes the responses.
 */
@Service
@RequiredArgsConstructor
public class SearchService {

    private final ElasticsearchGateway es;

    /** Full-text search over descriptions, optionally filtered by type and recency. */
    public SearchResult search(String text, String type, Integer days, int size) {
        ObjectNode bool = es.newObject();
        ArrayNode must = bool.putArray("must");
        ArrayNode filter = bool.putArray("filter");

        boolean hasText = text != null && !text.isBlank();
        if (hasText) {
            must.addObject().putObject("match").put("description", text);
        } else {
            must.addObject().putObject("match_all");
        }
        addFilters(filter, type, days);

        ObjectNode query = es.newObject();
        query.set("bool", bool);

        ObjectNode body = es.newObject();
        body.put("size", clampSize(size));
        body.set("query", query);
        // Rank a text search by relevance; browse (no text) by recency.
        String sortField = hasText ? "_score" : "reportedAt";
        body.putArray("sort").addObject().putObject(sortField).put("order", "desc");
        return toResult(es.search(body));
    }

    /**
     * Past incidents within {@code radiusMeters} of a point ("3rd robbery at this bank in
     * 2 months"): a geo_distance filter plus optional type / recency, newest first.
     */
    public SearchResult nearby(double lat, double lon, int radiusMeters, String type, Integer days, int size) {
        ObjectNode bool = es.newObject();
        ArrayNode filter = bool.putArray("filter");

        ObjectNode geo = filter.addObject().putObject("geo_distance");
        geo.put("distance", radiusMeters + "m");
        geo.putObject("location").put("lat", lat).put("lon", lon);
        addFilters(filter, type, days);

        ObjectNode query = es.newObject();
        query.set("bool", bool);

        ObjectNode body = es.newObject();
        body.put("size", clampSize(size));
        body.set("query", query);
        body.putArray("sort").addObject().putObject("reportedAt").put("order", "desc");
        return toResult(es.search(body));
    }

    private void addFilters(ArrayNode filter, String type, Integer days) {
        if (type != null && !type.isBlank()) {
            filter.addObject().putObject("term").put("type", type.trim().toUpperCase());
        }
        if (days != null && days > 0) {
            long from = Instant.now().minus(days, ChronoUnit.DAYS).toEpochMilli();
            ObjectNode range = filter.addObject().putObject("range").putObject("reportedAt");
            range.put("gte", from);
        }
    }

    private SearchResult toResult(JsonNode response) {
        long total = response.path("hits").path("total").path("value").asLong(0);
        List<Hit> hits = new ArrayList<>();
        for (JsonNode h : response.path("hits").path("hits")) {
            JsonNode s = h.path("_source");
            JsonNode loc = s.path("location");
            hits.add(new Hit(
                    s.path("cacheId").asText(""),
                    s.path("type").asText(""),
                    s.path("description").asText(""),
                    s.path("address").asText(""),
                    loc.path("lat").asDouble(0), loc.path("lon").asDouble(0),
                    s.path("reportedAt").asLong(0),
                    h.path("_score").isNumber() ? h.path("_score").asDouble() : null));
        }
        return new SearchResult(total, hits);
    }

    private static int clampSize(int size) {
        return Math.max(1, Math.min(size, 200));
    }

    public record Hit(String cacheId, String type, String description, String address,
                      double latitude, double longitude, long reportedAt, Double score) {}

    public record SearchResult(long total, List<Hit> hits) {}
}
