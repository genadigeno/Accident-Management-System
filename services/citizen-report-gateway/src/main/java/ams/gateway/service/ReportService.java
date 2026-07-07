package ams.gateway.service;

import ams.data.model.AccidentEventModel;
import ams.data.model.AccidentType;
import ams.data.model.Location;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import org.springframework.web.server.ResponseStatusException;

import java.time.Duration;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Intake pipeline for citizen reports: validate → rate-limit per reporter → duplicate hint
 * (best-effort call to the correlation service) → publish to the source topic (confirmed
 * send: if Kafka does not ack, the caller gets 503 and the report is NOT accepted).
 */
@Slf4j
@Service
public class ReportService {

    private final KafkaTemplate<String, AccidentEventModel> kafkaTemplate;
    private final MeterRegistry meterRegistry;
    private final RestClient correlationClient;
    private final String sourceTopic;
    private final String correlationUrl;
    private final int ratePerMinute;

    /** reporter (api key or client address) -> reports in the current minute window. */
    private final Cache<String, AtomicInteger> rateWindows = Caffeine.newBuilder()
            .expireAfterWrite(Duration.ofMinutes(1))
            .maximumSize(10_000)
            .build();

    public ReportService(KafkaTemplate<String, AccidentEventModel> kafkaTemplate,
                         MeterRegistry meterRegistry,
                         @Value("${kafka.source.topic}") String sourceTopic,
                         @Value("${gateway.correlation.url:}") String correlationUrl,
                         @Value("${gateway.correlation.timeout-ms:1000}") int correlationTimeoutMs,
                         @Value("${gateway.rate-limit-per-minute:10}") int ratePerMinute) {
        this.kafkaTemplate = kafkaTemplate;
        this.meterRegistry = meterRegistry;
        this.sourceTopic = sourceTopic;
        this.correlationUrl = correlationUrl;
        this.ratePerMinute = ratePerMinute;
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(correlationTimeoutMs);
        factory.setReadTimeout(correlationTimeoutMs);
        this.correlationClient = RestClient.builder().requestFactory(factory).build();
    }

    public AcceptedReport accept(ReportDraft draft, String reporter) {
        AccidentType type = validate(draft);

        if (rateWindows.get(reporter, k -> new AtomicInteger()).incrementAndGet() > ratePerMinute) {
            meterRegistry.counter("ams.gateway.rate_limited").increment();
            throw new ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS,
                    "rate limit exceeded (" + ratePerMinute + " reports/minute)");
        }

        DuplicateHint duplicateOf = duplicateHint(type, draft.latitude(), draft.longitude());

        String reportId = UUID.randomUUID().toString();
        AccidentEventModel event = AccidentEventModel.newBuilder()
                .setId(ThreadLocalRandom.current().nextInt(1_000_000))
                .setType(type)
                .setDate(LocalDate.now())
                .setDescription(draft.description().strip())
                .setCacheId(reportId)
                .setLocationBuilder(Location.newBuilder()
                        .setAddress(draft.address().strip())
                        .setLatitude(Double.toString(draft.latitude()))
                        .setLongitude(Double.toString(draft.longitude())))
                .build();
        try {
            kafkaTemplate.send(sourceTopic, reportId, event).get(5, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                    "report intake is temporarily unavailable");
        } catch (Exception ex) {
            log.error("could not publish report: {}", ex.getMessage());
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                    "report intake is temporarily unavailable");
        }
        meterRegistry.counter("ams.gateway.reports_accepted").increment();

        String message = duplicateOf == null
                ? "Report accepted. Responders are being dispatched if required."
                : "Report accepted. This incident appears to be already reported ("
                  + duplicateOf.reportCount() + " report(s)) — your information has been added.";
        return new AcceptedReport(reportId, duplicateOf, message);
    }

    private AccidentType validate(ReportDraft draft) {
        if (draft == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "body is required");
        }
        AccidentType type;
        try {
            type = AccidentType.valueOf(String.valueOf(draft.type()).trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "type must be one of CRIMINAL, CAR_ACCIDENT, OTHER_ACCIDENT, FIRE_ACCIDENT");
        }
        if (draft.address() == null || draft.address().isBlank() || draft.address().length() > 255) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "address is required (max 255 chars)");
        }
        if (draft.description() == null || draft.description().isBlank() || draft.description().length() > 500) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "description is required (max 500 chars)");
        }
        if (draft.latitude() < -90 || draft.latitude() > 90) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "latitude must be between -90 and 90");
        }
        if (draft.longitude() < -180 || draft.longitude() > 180) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "longitude must be between -180 and 180");
        }
        return type;
    }

    /** Best-effort: a correlation-service outage must never block report intake. */
    @SuppressWarnings("unchecked")
    private DuplicateHint duplicateHint(AccidentType type, double lat, double lng) {
        if (correlationUrl.isBlank()) {
            return null;
        }
        try {
            List<Map<String, Object>> incidents = correlationClient.get()
                    .uri(correlationUrl + "/api/v1/incidents/nearby?lat={lat}&lng={lng}&type={type}",
                            lat, lng, type.name())
                    .retrieve()
                    .body(List.class);
            if (incidents == null || incidents.isEmpty()) {
                return null;
            }
            Map<String, Object> incident = incidents.get(0);
            return new DuplicateHint(
                    String.valueOf(incident.get("id")),
                    ((Number) incident.getOrDefault("reportCount", 0)).intValue());
        } catch (Exception ex) {
            log.debug("duplicate hint unavailable: {}", ex.getMessage());
            return null;
        }
    }

    public record ReportDraft(String type, String address, double latitude, double longitude,
                              String description) {}

    public record DuplicateHint(String incidentId, int reportCount) {}

    public record AcceptedReport(String reportId, DuplicateHint duplicateOf, String message) {}
}
