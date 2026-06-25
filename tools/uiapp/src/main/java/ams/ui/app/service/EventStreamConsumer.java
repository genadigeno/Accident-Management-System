package ams.ui.app.service;

import ams.data.model.AccidentEventModel;
import ams.ui.app.dta.AccidentEventDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

/**
 * Consumes the live accident event stream and forwards each event to connected dashboard
 * browsers over WebSocket ({@code /topic/events}), while feeding the {@link AnalyticsService}.
 * Also tallies the downstream sensitive-zone and fraud signals for the analytics panel.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class EventStreamConsumer {

    private final SimpMessagingTemplate messagingTemplate;
    private final AnalyticsService analyticsService;

    @KafkaListener(topics = "${topic.config.source}", groupId = "${spring.kafka.consumer.group-id}")
    public void onEvent(AccidentEventModel event) {
        String type = event.getType() != null ? event.getType().name() : "UNKNOWN";
        String address = str(event.getLocation() != null ? event.getLocation().getAddress() : null);
        AccidentEventDto dto = new AccidentEventDto(
                str(event.getCacheId()),
                type,
                address,
                str(event.getLocation() != null ? event.getLocation().getLatitude() : null),
                str(event.getLocation() != null ? event.getLocation().getLongitude() : null),
                event.getDate() != null ? event.getDate().toString() : "",
                System.currentTimeMillis());
        messagingTemplate.convertAndSend("/topic/events", dto);
        analyticsService.recordEvent(type, address);
    }

    @KafkaListener(topics = "${topic.config.sensitive}", groupId = "${spring.kafka.consumer.group-id}-sensitive")
    public void onSensitive(Object payload) {
        analyticsService.recordSensitive();
    }

    @KafkaListener(topics = "${topic.config.fraud}", groupId = "${spring.kafka.consumer.group-id}-fraud")
    public void onFraud(Object payload) {
        analyticsService.recordFraud();
    }

    private static String str(CharSequence cs) {
        return cs == null ? "" : cs.toString();
    }
}
