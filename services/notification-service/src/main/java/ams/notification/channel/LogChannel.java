package ams.notification.channel;

import ams.notification.domain.Notification;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/** Always-on channel: writes every notification to the service log at WARN. */
@Slf4j
@Component
public class LogChannel implements NotificationChannel {

    @Override
    public String name() {
        return "log";
    }

    @Override
    public boolean send(Notification n) {
        log.warn("[{}][{}] {} — {} (incident {})",
                n.getSource(), n.getSeverity(), n.getTitle(), n.getMessage(), n.getIncidentId());
        return true;
    }
}
