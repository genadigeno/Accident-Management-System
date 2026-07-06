package ams.notification.channel;

import ams.notification.domain.Notification;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.Map;

/**
 * POSTs each notification as JSON to a configured URL (Slack/Teams/incident-tooling style).
 * Enabled only when {@code notification.webhook.url} is set. Failures are recorded per
 * notification, never retried here — the webhook receiver is expected to be near.
 */
@Slf4j
@Component
@ConditionalOnProperty("notification.webhook.url")
public class WebhookChannel implements NotificationChannel {

    private final RestClient restClient;
    private final String url;

    public WebhookChannel(
            @Value("${notification.webhook.url}") String url,
            @Value("${notification.webhook.timeout-ms:3000}") int timeoutMs) {
        this.url = url;
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(timeoutMs);
        factory.setReadTimeout(timeoutMs);
        this.restClient = RestClient.builder().requestFactory(factory).build();
        log.info("Webhook channel enabled -> {}", url);
    }

    @Override
    public String name() {
        return "webhook";
    }

    @Override
    public boolean send(Notification n) {
        try {
            restClient.post()
                    .uri(url)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(Map.of(
                            "source", n.getSource(),
                            "severity", n.getSeverity(),
                            "title", n.getTitle(),
                            "message", n.getMessage() == null ? "" : n.getMessage(),
                            "incidentId", n.getIncidentId() == null ? "" : n.getIncidentId(),
                            "createdAt", n.getCreatedAt().toString()))
                    .retrieve()
                    .toBodilessEntity();
            return true;
        } catch (Exception ex) {
            log.warn("webhook delivery failed: {}", ex.getMessage());
            return false;
        }
    }
}
