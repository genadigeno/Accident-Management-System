package ams.ui.app.service;

import ams.ui.app.dta.AnalyticsSnapshot;
import ams.ui.app.dta.AnalyticsSnapshot.LocationCount;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.RequiredArgsConstructor;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Maintains real-time aggregates over the accident stream (throughput, per-type counts, top
 * locations, and downstream sensitive/fraud signals) and publishes a snapshot every second to
 * {@code /topic/analytics}. Counters are fed by {@code EventStreamConsumer}.
 */
@Service
@RequiredArgsConstructor
public class AnalyticsService {

    static final String TOPIC = "/topic/analytics";

    private final SimpMessagingTemplate messagingTemplate;

    private final AtomicLong total = new AtomicLong();
    private final Map<String, AtomicLong> byType = new ConcurrentHashMap<>();
    // Bounded: unlimited per-location counters were a slow memory leak under sustained load
    // (address cardinality is unbounded). The dashboard only shows the top locations, so
    // evicting the long tail is acceptable — top-N is approximate once eviction kicks in.
    private final Cache<String, AtomicLong> locations = Caffeine.newBuilder()
            .maximumSize(10_000)
            .build();
    private final AtomicLong sensitive = new AtomicLong();
    private final AtomicLong fraud = new AtomicLong();

    private volatile long lastTotal = 0;
    private volatile long lastSnapshotAt = System.currentTimeMillis();

    public void recordEvent(String type, String address) {
        total.incrementAndGet();
        byType.computeIfAbsent(type == null ? "UNKNOWN" : type, k -> new AtomicLong()).incrementAndGet();
        if (address != null && !address.isBlank()) {
            locations.get(address, k -> new AtomicLong()).incrementAndGet();
        }
    }

    public void recordSensitive() {
        sensitive.incrementAndGet();
    }

    public void recordFraud() {
        fraud.incrementAndGet();
    }

    @Scheduled(fixedRate = 1000)
    public void publish() {
        long now = System.currentTimeMillis();
        long current = total.get();
        double seconds = Math.max(1L, now - lastSnapshotAt) / 1000.0;
        double rate = (current - lastTotal) / seconds;
        lastTotal = current;
        lastSnapshotAt = now;

        Map<String, Long> types = new LinkedHashMap<>();
        byType.forEach((k, v) -> types.put(k, v.get()));

        List<LocationCount> top = locations.asMap().entrySet().stream()
                .sorted(Comparator.comparingLong((Map.Entry<String, AtomicLong> e) -> e.getValue().get()).reversed())
                .limit(5)
                .map(e -> new LocationCount(e.getKey(), e.getValue().get()))
                .toList();

        messagingTemplate.convertAndSend(TOPIC, new AnalyticsSnapshot(
                current, rate, types, top, sensitive.get(), fraud.get(), now));
    }
}
