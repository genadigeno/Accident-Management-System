package ams.search.health;

import ams.search.es.ElasticsearchGateway;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Surfaces Elasticsearch reachability under {@code /actuator/health}. Reported as a separate
 * component so ES being down is visible without failing the service's own liveness (indexing
 * simply retries until ES returns).
 */
@Component("elasticsearch")
@RequiredArgsConstructor
public class ElasticsearchHealthIndicator implements HealthIndicator {

    private final ElasticsearchGateway es;

    @Override
    public Health health() {
        Map<String, Object> details = es.health();
        Object status = details.get("clusterStatus");
        boolean up = status != null && !"unreachable".equals(status);
        return (up ? Health.up() : Health.down()).withDetails(details).build();
    }
}
