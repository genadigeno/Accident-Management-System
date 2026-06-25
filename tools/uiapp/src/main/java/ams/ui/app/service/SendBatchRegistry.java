package ams.ui.app.service;

import ams.ui.app.dta.SendBatchView;
import ams.ui.app.model.SendBatch;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.RequiredArgsConstructor;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Keeps the most recent "generate events" batches and pushes their status to the dashboard
 * over WebSocket ({@code /topic/send-status}). Batches are held in a bounded Caffeine cache.
 */
@Service
@RequiredArgsConstructor
public class SendBatchRegistry {

    static final String TOPIC = "/topic/send-status";

    private final SimpMessagingTemplate messagingTemplate;

    private final Cache<String, SendBatch> batches = Caffeine.newBuilder()
            .maximumSize(200)
            .expireAfterWrite(Duration.ofHours(1))
            .build();

    /** Creates a new batch, stores it, and announces it to the dashboard. */
    public SendBatch create(int total) {
        SendBatch batch = new SendBatch(UUID.randomUUID().toString(), total);
        batches.put(batch.getId(), batch);
        publish(batch);
        return batch;
    }

    /** Records one producer result; publishes a final update when the batch completes. */
    public void recordResult(SendBatch batch, boolean success) {
        if (batch.recordResult(success)) {
            publish(batch);
        }
    }

    public List<SendBatchView> recent() {
        return batches.asMap().values().stream()
                .sorted(Comparator.comparingLong(SendBatch::getStartedAt).reversed())
                .map(SendBatchView::from)
                .toList();
    }

    public Optional<SendBatchView> get(String id) {
        return Optional.ofNullable(batches.getIfPresent(id)).map(SendBatchView::from);
    }

    /** Pushes live progress for in-flight batches so the dashboard progress bars move. */
    @Scheduled(fixedRate = 500)
    public void publishInFlight() {
        batches.asMap().values().stream()
                .filter(b -> b.getStatus() == SendBatch.Status.IN_PROGRESS)
                .forEach(this::publish);
    }

    private void publish(SendBatch batch) {
        messagingTemplate.convertAndSend(TOPIC, SendBatchView.from(batch));
    }
}
