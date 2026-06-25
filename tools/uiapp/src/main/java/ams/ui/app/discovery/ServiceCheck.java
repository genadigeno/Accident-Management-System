package ams.ui.app.discovery;

import ams.ui.app.config.DiscoveryProperties;
import ams.ui.app.dta.ServiceHealthView;
import ams.ui.app.service.RegistratorService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.Map;

/**
 * Active service discovery: on a fixed schedule, probes each monitored service's
 * {@code /actuator/health}, measures latency, and classifies it UP / DEGRADED / DOWN. The
 * resulting snapshot is stored in the registry and pushed to the dashboard over WebSocket
 * ({@code /topic/service-discovery}).
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

    @Scheduled(fixedRateString = "${discovery.poll-interval-ms:5000}")
    public void pollAll() {
        for (Map.Entry<String, String> target : registratorService.targets().entrySet()) {
            registratorService.updateHealth(probe(target.getKey(), target.getValue()));
        }
        messagingTemplate.convertAndSend(TOPIC, registratorService.snapshot());
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
            return new ServiceHealthView(name, baseUrl, status, latency, code, System.currentTimeMillis(), detail);
        } catch (Exception ex) {
            long latency = System.currentTimeMillis() - start;
            return new ServiceHealthView(name, baseUrl, "DOWN", latency, null,
                    System.currentTimeMillis(), ex.getClass().getSimpleName());
        }
    }
}
