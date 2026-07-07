package ams.search.es;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.Map;

/**
 * Talks to Elasticsearch over its HTTP API using a plain {@link RestClient} + JSON, rather than
 * the typed Java client. This keeps the service decoupled from the ES client/server version pair
 * (the project's ES server is 9.x while Spring Boot ships an 8.x typed client): the index / search
 * / geo_distance REST endpoints are stable across those versions.
 */
@Slf4j
@Component
public class ElasticsearchGateway {

    private final RestClient es;
    private final ObjectMapper mapper;
    private final String index;

    public ElasticsearchGateway(
            @Value("${elasticsearch.url:http://localhost:9200}") String url,
            @Value("${elasticsearch.index:ams-incidents}") String index,
            @Value("${elasticsearch.connect-timeout-ms:3000}") int connectTimeoutMs,
            @Value("${elasticsearch.read-timeout-ms:10000}") int readTimeoutMs,
            ObjectMapper mapper) {
        this.index = index;
        this.mapper = mapper;
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(connectTimeoutMs);
        factory.setReadTimeout(readTimeoutMs);
        this.es = RestClient.builder().baseUrl(url).requestFactory(factory).build();
        log.info("Elasticsearch gateway -> {} (index {})", url, index);
    }

    /**
     * Creates the incident index with an explicit mapping if it does not already exist. Best
     * effort at startup: if ES is not up yet this logs and returns; the first indexing attempt
     * (and its retry) will create it. Never fails the application context.
     */
    @PostConstruct
    void ensureIndex() {
        try {
            boolean exists = Boolean.TRUE.equals(es.head().uri("/{index}", index)
                    .exchange((req, res) -> res.getStatusCode().is2xxSuccessful()));
            if (exists) {
                return;
            }
            String mapping = """
                    {
                      "mappings": {
                        "properties": {
                          "cacheId":     { "type": "keyword" },
                          "type":        { "type": "keyword" },
                          "description": { "type": "text" },
                          "address":     { "type": "text", "fields": { "raw": { "type": "keyword" } } },
                          "location":    { "type": "geo_point" },
                          "reportedAt":  { "type": "date", "format": "epoch_millis" }
                        }
                      }
                    }""";
            es.put().uri("/{index}", index)
                    .contentType(MediaType.APPLICATION_JSON).body(mapping)
                    .retrieve().toBodilessEntity();
            log.info("Created Elasticsearch index {}", index);
        } catch (Exception e) {
            log.warn("Could not ensure index {} at startup ({}); will retry on first write",
                    index, e.getMessage());
        }
    }

    /** Idempotent upsert of one incident document (id = cacheId). */
    public void index(IncidentDocument doc) {
        try {
            es.put().uri("/{index}/_doc/{id}", index, doc.cacheId())
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(mapper.writeValueAsString(doc))
                    .retrieve().toBodilessEntity();
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalStateException("could not serialize incident document", e);
        }
    }

    /** Runs a raw query body against {@code _search} and returns the parsed response. */
    public JsonNode search(ObjectNode queryBody) {
        String body = es.post().uri("/{index}/_search", index)
                .contentType(MediaType.APPLICATION_JSON)
                .body(queryBody.toString())
                .retrieve()
                .body(String.class);
        try {
            return mapper.readTree(body);
        } catch (Exception e) {
            throw new IllegalStateException("could not parse Elasticsearch response", e);
        }
    }

    public ObjectNode newObject() {
        return mapper.createObjectNode();
    }

    /** ES accepts an index that does not exist yet on search by returning zero hits only if
     *  {@code allow_no_indices}; to keep the API simple we ensure existence lazily here. */
    public boolean indexExists() {
        try {
            return Boolean.TRUE.equals(es.head().uri("/{index}", index)
                    .exchange((req, res) -> res.getStatusCode().is2xxSuccessful()));
        } catch (Exception e) {
            return false;
        }
    }

    public Map<String, Object> health() {
        try {
            String body = es.get().uri("/_cluster/health").retrieve().body(String.class);
            JsonNode node = mapper.readTree(body);
            return Map.of("clusterStatus", node.path("status").asText("unknown"),
                    "index", index, "indexExists", indexExists());
        } catch (Exception e) {
            return Map.of("clusterStatus", "unreachable", "error", String.valueOf(e.getMessage()));
        }
    }
}
