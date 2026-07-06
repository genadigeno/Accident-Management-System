package ams.notification.service;

import ams.notification.channel.NotificationChannel;
import ams.notification.domain.Notification;
import ams.notification.domain.NotificationRepository;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Records every incoming alert exactly once and fans it out to the enabled channels.
 *
 * <ul>
 *   <li><b>Dedup</b> is database-backed: the unique {@code dedup_key} makes redeliveries and
 *       full-topic replays (fresh consumer group, DLT replays) idempotent forever.</li>
 *   <li><b>Rate limiting</b> is per source per minute: an alert storm (e.g. a fraud attack)
 *       is still recorded, but stops hammering the channels beyond the configured rate.</li>
 * </ul>
 */
@Slf4j
@Service
public class NotificationService {

    private final NotificationRepository repository;
    private final List<NotificationChannel> channels;
    private final MeterRegistry meterRegistry;
    private final int ratePerMinute;

    /** source -> notifications sent to channels in the current minute window. */
    private final Cache<String, AtomicInteger> rateWindows = Caffeine.newBuilder()
            .expireAfterWrite(Duration.ofMinutes(1))
            .maximumSize(100)
            .build();

    public NotificationService(NotificationRepository repository,
                               List<NotificationChannel> channels,
                               MeterRegistry meterRegistry,
                               @Value("${notification.rate-limit-per-minute:30}") int ratePerMinute) {
        this.repository = repository;
        this.channels = channels;
        this.meterRegistry = meterRegistry;
        this.ratePerMinute = ratePerMinute;
        log.info("Notification channels enabled: {}",
                channels.stream().map(NotificationChannel::name).toList());
    }

    @Transactional
    public void record(String source, String severity, String title, String message,
                       String incidentId, String dedupKey) {
        if (repository.existsByDedupKey(dedupKey)) {
            meterRegistry.counter("ams.notifications.deduplicated", "source", source).increment();
            return;
        }
        Notification notification = new Notification();
        notification.setDedupKey(dedupKey);
        notification.setSource(source);
        notification.setSeverity(severity);
        notification.setTitle(truncate(title, 200));
        notification.setMessage(truncate(message, 1000));
        notification.setIncidentId(incidentId);
        notification.setCreatedAt(Instant.now());

        boolean withinRate = rateWindows.get(source, k -> new AtomicInteger())
                .incrementAndGet() <= ratePerMinute;
        notification.setRateLimited(!withinRate);
        if (withinRate) {
            notification.setChannels(deliver(notification));
            meterRegistry.counter("ams.notifications.sent", "source", source).increment();
        } else {
            meterRegistry.counter("ams.notifications.rate_limited", "source", source).increment();
        }
        repository.save(notification);
    }

    private String deliver(Notification notification) {
        return channels.stream()
                .map(channel -> channel.name() + ":" + (channel.send(notification) ? "sent" : "failed"))
                .collect(Collectors.joining(","));
    }

    private static String truncate(String value, int max) {
        if (value == null) {
            return null;
        }
        return value.length() <= max ? value : value.substring(0, max - 1) + "…";
    }
}
