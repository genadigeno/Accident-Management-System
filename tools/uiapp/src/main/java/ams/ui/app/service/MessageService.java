package ams.ui.app.service;

import ams.data.model.AccidentEventModel;
import ams.data.model.AccidentType;
import ams.data.model.Location;
import ams.ui.app.dta.MessageRequest;
import ams.ui.app.model.SendBatch;
import ams.ui.app.util.LocationPropertyGenerator;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageService {

    private static final int MAX_TOTAL = 1_000_000;
    private static final int MAX_RATE = 100_000;

    private final KafkaTemplate<String, AccidentEventModel> kafkaTemplate;
    private final SendBatchRegistry batchRegistry;
    private final Random random = new Random();
    // Bounded queue + caller-runs: under a flood of generate requests the REST thread dispatches
    // inline (natural backpressure) instead of queueing batches without limit on the heap.
    private final ExecutorService dispatchExecutor = new ThreadPoolExecutor(
            3, 3, 0L, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(50),
            new ThreadPoolExecutor.CallerRunsPolicy());
    // Paces the FIXED_RATE / RANGE_RATE modes: one second-quantum quota per tick.
    private final ScheduledExecutorService pacer = Executors.newScheduledThreadPool(2, runnable -> {
        Thread thread = new Thread(runnable, "msg-pacer");
        thread.setDaemon(true);
        return thread;
    });

    @Value("${topic.config.source}")
    private String topicName;

    /**
     * Registers a batch and dispatches it according to the request's mode, returning immediately
     * so the caller (and the dashboard) can track progress. Each record's producer ack/failure is
     * reported back to the {@link SendBatchRegistry}, so the batch progress reflects real delivery.
     */
    public SendBatch send(MessageRequest req) {
        int total = req.getTotal();
        if (total < 1 || total > MAX_TOTAL) {
            throw new IllegalArgumentException("Total number of messages must be between 1 and " + MAX_TOTAL);
        }
        String mode = req.getMode() == null ? "AT_ONCE" : req.getMode().trim().toUpperCase();
        SendBatch batch = batchRegistry.create(total);
        switch (mode) {
            case "AT_ONCE" -> dispatchExecutor.execute(() -> dispatchBurst(batch, total));
            case "FIXED_RATE" -> {
                int rate = req.getRatePerSecond();
                if (rate < 1 || rate > MAX_RATE) {
                    throw new IllegalArgumentException("ratePerSecond must be between 1 and " + MAX_RATE);
                }
                schedulePaced(batch, total, () -> rate);
            }
            case "RANGE_RATE" -> {
                int min = req.getRateMin();
                int max = req.getRateMax();
                if (min < 1 || max < min || max > MAX_RATE) {
                    throw new IllegalArgumentException("require 1 <= rateMin <= rateMax <= " + MAX_RATE);
                }
                schedulePaced(batch, total, () -> min + random.nextInt(max - min + 1));
            }
            default -> throw new IllegalArgumentException("mode must be AT_ONCE, FIXED_RATE or RANGE_RATE");
        }
        return batch;
    }

    /** AT_ONCE: fire every event as fast as the producer accepts them. */
    private void dispatchBurst(SendBatch batch, int total) {
        for (int i = 0; i < total; i++) {
            sendOne(batch);
        }
        log.info("batch {}: {} messages enqueued at once to topic {}", batch.getId(), total, topicName);
    }

    /** FIXED_RATE / RANGE_RATE: send {@code quota} events each second until {@code total} is reached. */
    private void schedulePaced(SendBatch batch, int total, IntSupplier quotaPerSecond) {
        AtomicInteger remaining = new AtomicInteger(total);
        ScheduledFuture<?>[] handle = new ScheduledFuture<?>[1];
        handle[0] = pacer.scheduleAtFixedRate(() -> {
            try {
                int rem = remaining.get();
                if (rem <= 0) {
                    handle[0].cancel(false);
                    return;
                }
                int toSend = Math.min(Math.max(1, quotaPerSecond.getAsInt()), rem);
                for (int i = 0; i < toSend; i++) {
                    sendOne(batch);
                }
                if (remaining.addAndGet(-toSend) <= 0) {
                    handle[0].cancel(false);
                    log.info("batch {}: {} messages sent (paced) to topic {}", batch.getId(), total, topicName);
                }
            } catch (Exception e) {
                log.error("paced dispatch for batch {} failed: {}", batch.getId(), e.getMessage());
                handle[0].cancel(false);
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    private void sendOne(SendBatch batch) {
        AccidentEventModel model = buildRandomEvent();
        kafkaTemplate.send(topicName, model.getCacheId().toString(), model)
                .whenComplete((result, ex) -> batchRegistry.recordResult(batch, ex == null));
    }

    private AccidentEventModel buildRandomEvent() {
        AccidentEventModel model = new AccidentEventModel();
        model.setId(random.nextInt(1_000_000));
        model.setCacheId(UUID.randomUUID().toString());
        model.setDate(LocalDate.now());
        Location location = new Location();
        location.setAddress(LocationPropertyGenerator.generateAddress());
        location.setLatitude(LocationPropertyGenerator.generateLatitude());
        location.setLongitude(LocationPropertyGenerator.generateLongitude());
        model.setLocation(location);
        model.setType(getRandomAccidentType());
        model.setDescription(randomDescription());
        return model;
    }

    private AccidentType getRandomAccidentType() {
        return AccidentType.values()[random.nextInt(AccidentType.values().length)];
    }

    @PreDestroy
    void shutdown() {
        dispatchExecutor.shutdown();
        pacer.shutdownNow();
    }

    /**
     * Realistic incident descriptions. A few deliberately contain BOLO keywords (gun, robbery,
     * stolen vehicle) so the downstream law-enforcement / fraud / geo-fence features light up
     * naturally under random load.
     */
    private static final String[] DESCRIPTIONS = {
            "minor fender bender", "multi-vehicle collision", "hit and run", "vehicle rollover",
            "kitchen fire reported", "building fire, smoke visible", "electrical fire",
            "armed robbery in progress", "suspect seen with a gun", "stolen vehicle pursuit",
            "burglary reported", "noise complaint", "suspicious package", "medical emergency",
            "street flooding", "downed power line", "gas leak reported", "pedestrian struck"
    };

    private String randomDescription() {
        return DESCRIPTIONS[random.nextInt(DESCRIPTIONS.length)];
    }
}
