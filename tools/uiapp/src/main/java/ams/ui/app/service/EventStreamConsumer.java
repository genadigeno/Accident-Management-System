package ams.ui.app.service;

import ams.data.model.AccidentEventModel;
import ams.data.model.AlertEvent;
import ams.ui.app.dta.AccidentEventDto;
import ams.ui.app.dta.AlertDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Consumes the live accident event stream, feeds the {@link AnalyticsService}, and forwards
 * events to connected dashboard browsers over WebSocket ({@code /topic/events}).
 *
 * <p>Events are batched: sending one STOMP frame per event melted the in-memory broker and the
 * browser at load-test rates (thousands of events/second). Instead, events are buffered and
 * flushed as ONE array frame every {@value #FLUSH_INTERVAL_MS}ms, capped at the most recent
 * {@value #MAX_EVENTS_PER_FLUSH} — the feed only displays the latest 50 anyway, and the
 * analytics counters are fed per event regardless of what is dropped from the feed.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class EventStreamConsumer {

    static final int FLUSH_INTERVAL_MS = 250;
    static final int MAX_EVENTS_PER_FLUSH = 200;

    private final SimpMessagingTemplate messagingTemplate;
    private final AnalyticsService analyticsService;

    private final ConcurrentLinkedQueue<AccidentEventDto> pending = new ConcurrentLinkedQueue<>();

    @KafkaListener(topics = "${topic.config.source}", groupId = "${spring.kafka.consumer.group-id}")
    public void onEvent(AccidentEventModel event) {
        String type = event.getType() != null ? event.getType().name() : "UNKNOWN";
        String address = str(event.getLocation() != null ? event.getLocation().getAddress() : null);
        analyticsService.recordEvent(type, address);
        pending.add(new AccidentEventDto(
                str(event.getCacheId()),
                type,
                address,
                str(event.getLocation() != null ? event.getLocation().getLatitude() : null),
                str(event.getLocation() != null ? event.getLocation().getLongitude() : null),
                event.getDate() != null ? event.getDate().toString() : "",
                System.currentTimeMillis()));
    }

    /** Drains the buffer and pushes one array frame; keeps only the newest events under load. */
    @Scheduled(fixedRate = FLUSH_INTERVAL_MS)
    public void flush() {
        if (pending.isEmpty()) {
            return;
        }
        List<AccidentEventDto> drained = new ArrayList<>();
        AccidentEventDto dto;
        while ((dto = pending.poll()) != null) {
            drained.add(dto);
        }
        if (drained.size() > MAX_EVENTS_PER_FLUSH) {
            log.debug("live feed: dropping {} old event(s) from an oversized flush",
                    drained.size() - MAX_EVENTS_PER_FLUSH);
            drained = drained.subList(drained.size() - MAX_EVENTS_PER_FLUSH, drained.size());
        }
        messagingTemplate.convertAndSend("/topic/events", drained);
    }

    @KafkaListener(topics = "${topic.config.sensitive}", groupId = "${spring.kafka.consumer.group-id}-sensitive")
    public void onSensitive(AccidentEventModel event) {
        analyticsService.recordSensitive();
        String address = str(event.getLocation() != null ? event.getLocation().getAddress() : null);
        pushAlert("GEOFENCE", "HIGH", "Incident in sensitive zone",
                str(event.getType() != null ? event.getType().name() : "") + " at " + address);
    }

    @KafkaListener(topics = "${topic.config.fraud}", groupId = "${spring.kafka.consumer.group-id}-fraud")
    public void onFraud(Object payload) {
        analyticsService.recordFraud();
        pushAlert("FRAUD", "HIGH", "Possible fraud: rapid repeat reports", str(payload.toString()));
    }

    /** Structured BOLO alerts from law-enforcement. */
    @KafkaListener(topics = "${topic.config.bolo}", groupId = "${spring.kafka.consumer.group-id}-bolo")
    public void onBolo(AlertEvent alert) {
        pushAlert(alert);
    }

    /** Structured response-SLA breach alerts from emergency. */
    @KafkaListener(topics = "${topic.config.sla}", groupId = "${spring.kafka.consumer.group-id}-sla")
    public void onSla(AlertEvent alert) {
        pushAlert(alert);
    }

    private void pushAlert(AlertEvent alert) {
        pushAlert(str(alert.getSource()), alert.getSeverity() != null ? alert.getSeverity().name() : "INFO",
                str(alert.getTitle()), str(alert.getMessage()));
    }

    private void pushAlert(String source, String severity, String title, String message) {
        messagingTemplate.convertAndSend("/topic/alerts",
                new AlertDto(source, severity, title, message, System.currentTimeMillis()));
    }

    private static String str(CharSequence cs) {
        return cs == null ? "" : cs.toString();
    }
}
