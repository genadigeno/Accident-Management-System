package ams.notification.api;

import ams.notification.domain.Notification;
import ams.notification.domain.NotificationRepository;
import ams.notification.domain.NotificationRepository.SourceSeverityCount;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;

/** Read API over the notification history. */
@RestController
@RequestMapping("/api/v1/notifications")
@RequiredArgsConstructor
public class NotificationController {

    private final NotificationRepository repository;

    /** The 50 most recent notifications, newest first. */
    @GetMapping
    public List<NotificationView> recent() {
        return repository.findTop50ByOrderByIdDesc().stream().map(NotificationView::from).toList();
    }

    /** Counts per source and severity. */
    @GetMapping("/summary")
    public List<SummaryRow> summary() {
        return repository.countBySourceAndSeverity().stream()
                .map(row -> new SummaryRow(row.getSource(), row.getSeverity(), row.getTotal()))
                .toList();
    }

    public record NotificationView(long id, String source, String severity, String title,
                                   String message, String incidentId, String channels,
                                   boolean rateLimited, Instant createdAt) {
        static NotificationView from(Notification n) {
            return new NotificationView(n.getId(), n.getSource(), n.getSeverity(), n.getTitle(),
                    n.getMessage(), n.getIncidentId(), n.getChannels(), n.isRateLimited(), n.getCreatedAt());
        }
    }

    public record SummaryRow(String source, String severity, long total) {}
}
