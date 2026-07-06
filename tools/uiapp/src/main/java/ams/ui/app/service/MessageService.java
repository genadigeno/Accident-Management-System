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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageService {

    private final KafkaTemplate<String, AccidentEventModel> kafkaTemplate;
    private final SendBatchRegistry batchRegistry;
    private final Random random = new Random();
    // Bounded queue + caller-runs: under a flood of generate requests the REST thread dispatches
    // inline (natural backpressure) instead of queueing batches without limit on the heap.
    private final ExecutorService dispatchExecutor = new ThreadPoolExecutor(
            3, 3, 0L, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(50),
            new ThreadPoolExecutor.CallerRunsPolicy());

    @Value("${topic.config.source}")
    private String topicName;

    /**
     * Registers a batch and dispatches it asynchronously, returning immediately so the caller
     * (and the dashboard) can track progress. Each record's producer ack/failure is reported
     * back to the {@link SendBatchRegistry}.
     */
    public SendBatch send(MessageRequest messageRequest) {
        int total = messageRequest.getTotal();
        if (total < 1 || total > 10_000) {
            throw new IllegalArgumentException("Total number of messages must be between 1 and 10 000");
        }
        SendBatch batch = batchRegistry.create(total);
        dispatchExecutor.execute(() -> dispatch(batch, total));
        return batch;
    }

    private void dispatch(SendBatch batch, int total) {
        for (int i = 0; i < total; i++) {
            AccidentEventModel model = buildRandomEvent();
            kafkaTemplate.send(topicName, model.getCacheId().toString(), model)
                    .whenComplete((result, ex) -> batchRegistry.recordResult(batch, ex == null));
        }
        log.info("batch {}: {} messages enqueued to topic {}", batch.getId(), total, topicName);
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
