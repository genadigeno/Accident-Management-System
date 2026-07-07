package ams.ui.app.discovery;

import ams.ui.app.config.DiscoveryProperties;
import ams.ui.app.dta.ServiceHealthView;
import ams.ui.app.service.RegistratorService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Active service discovery: on a fixed schedule, probes each monitored service's
 * {@code /actuator/health}, measures latency, and classifies it UP / DEGRADED / DOWN. The
 * resulting snapshot is stored in the registry and pushed to the dashboard over WebSocket
 * ({@code /topic/service-discovery}).
 *
 * <p>Probes run in parallel: sequentially, one hung service (2s timeout each) delayed every
 * other probe and could overrun the whole polling cycle.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ServiceCheck {

    static final String TOPIC = "/topic/service-discovery";

    private final RegistratorService registratorService;
    private final DiscoveryProperties properties;
    private final SimpMessagingTemplate messagingTemplate;
    private final RestClient discoveryRestClient;
    private final ObjectMapper objectMapper;

    private final ExecutorService probePool = Executors.newFixedThreadPool(5, runnable -> {
        Thread thread = new Thread(runnable, "discovery-probe");
        thread.setDaemon(true);
        return thread;
    });

    @Scheduled(fixedRateString = "${discovery.poll-interval-ms:5000}")
    public void pollAll() {
        List<CompletableFuture<ServiceHealthView>> probes = registratorService.targets().entrySet().stream()
                .map(target -> CompletableFuture.supplyAsync(
                        () -> probe(target.getKey(), target.getValue()), probePool))
                .toList();
        probes.forEach(future -> registratorService.updateHealth(future.join()));
        messagingTemplate.convertAndSend(TOPIC, registratorService.snapshot());
    }

    @PreDestroy
    void shutdown() {
        probePool.shutdownNow();
    }

    private ServiceHealthView probe(String name, String baseUrl) {
        long start = System.currentTimeMillis();
        try {
            ResponseEntity<String> response = discoveryRestClient.get()
                    .uri(baseUrl + "/actuator/health")
                    .retrieve()
                    .toEntity(String.class);
            long latency = System.currentTimeMillis() - start;
            int code = response.getStatusCode().value();
            boolean bodyUp = response.getBody() != null && response.getBody().contains("\"status\":\"UP\"");

            String status;
            String detail = "";
            if (code == 200 && bodyUp) {
                if (latency > properties.getDegradedLatencyMs()) {
                    status = "DEGRADED";
                    detail = "slow: " + latency + "ms > " + properties.getDegradedLatencyMs() + "ms";
                } else {
                    status = "UP";
                }
            } else {
                status = "DOWN";
                detail = "health not UP (http " + code + ")";
            }
            // Throughput counters (only the event-consuming services expose these; others → null).
            Long received = code == 200 ? counter(baseUrl, "ams.events.received") : null;
            Long processed = code == 200 ? counter(baseUrl, "ams.events.processed") : null;
            return new ServiceHealthView(name, baseUrl, status, latency, code,
                    System.currentTimeMillis(), detail, received, processed);
        } catch (Exception ex) {
            long latency = System.currentTimeMillis() - start;
            return ServiceHealthView.of(name, baseUrl, "DOWN", latency, null,
                    System.currentTimeMillis(), ex.getClass().getSimpleName());
        }
    }

    /** Reads a Micrometer counter from a service's actuator; null if the service doesn't expose it. */
    private Long counter(String baseUrl, String metric) {
        try {
            String body = discoveryRestClient.get()
                    .uri(baseUrl + "/actuator/metrics/" + metric)
                    .retrieve()
                    .body(String.class);
            JsonNode value = objectMapper.readTree(body).path("measurements").path(0).path("value");
            return value.isNumber() ? value.asLong() : null;
        } catch (Exception e) {
            return null;   // metric not exposed by this service
        }
    }
}
