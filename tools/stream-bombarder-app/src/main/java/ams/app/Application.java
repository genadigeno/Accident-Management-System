package ams.app;

import ams.data.model.AccidentEventModel;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Load generator for the AMS pipeline. Produces transactional bursts of random
 * {@link AccidentEventModel} events to the source topic, until stopped (Ctrl-C) or until a
 * configured {@code --count} / {@code --duration-sec} limit is reached. Run several instances in
 * parallel to scale the load — each is an independent producer with its own transactional id.
 *
 * <p>Run with {@code --help} for the full list of options.
 */
public class Application {

    private static final Logger log = LoggerFactory.getLogger(Application.class);

    private static volatile boolean running = true;

    public static void main(String[] args) {
        if (BombarderConfig.wantsHelp(args)) {
            BombarderConfig.printHelp();
            return;
        }

        BombarderConfig config;
        try {
            config = BombarderConfig.from(args);
        } catch (IllegalArgumentException e) {
            System.err.println("error: " + e.getMessage());
            System.err.println("run with --help for usage.");
            System.exit(2);
            return;
        }

        log.info("stream-bombarder starting | {}", config.describe());

        KafkaProducer<String, AccidentEventModel> producer = createProducer(config);
        try {
            producer.initTransactions();
        } catch (Exception e) {
            log.error("could not initialise the Kafka producer at {} (schema registry {}): {}",
                    config.bootstrapServers(), config.schemaRegistryUrl(), e.toString());
            producer.close();
            System.exit(1);
            return;
        }

        // Register the shutdown hook only after a successful connect, so a failed start exits cleanly.
        CountDownLatch stopped = new CountDownLatch(1);
        AtomicBoolean finished = new AtomicBoolean(false);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (finished.get()) {
                return;   // normal completion already cleaned up
            }
            log.info("shutdown requested, finishing current burst...");
            running = false;
            try {
                stopped.await(15, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "bombarder-shutdown"));

        long totalSent = 0;
        try {
            totalSent = run(producer, config);
        } finally {
            finished.set(true);
            producer.close();
            stopped.countDown();
            log.info("stopped | {} event(s) sent in total", totalSent);
        }
    }

    private static long run(KafkaProducer<String, AccidentEventModel> producer, BombarderConfig config) {
        int effectiveMaxBurst = Math.max(1, (int) Math.round(config.maxBurst() * config.scale()));
        long deadline = config.durationSec() == 0
                ? Long.MAX_VALUE
                : System.nanoTime() + config.durationSec() * 1_000_000_000L;
        long totalSent = 0;

        while (running
                && (config.count() == 0 || totalSent < config.count())
                && System.nanoTime() < deadline) {

            if (!sleepBetweenBursts(config.intervalMs())) {
                break;   // interrupted
            }

            int burst = 1 + ThreadLocalRandom.current().nextInt(effectiveMaxBurst);
            if (config.count() > 0) {
                burst = (int) Math.min(burst, config.count() - totalSent);
            }

            try {
                sendBurst(producer, config.topic(), burst);
                totalSent += burst;
                log.info("sent {} event(s) | {} total", burst, totalSent);
            } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
                log.error("fatal producer error, stopping: {}", e.toString());
                break;
            } catch (KafkaException e) {
                log.warn("burst failed, aborting and continuing: {}", e.getMessage());
                abortQuietly(producer);
            }
        }
        return totalSent;
    }

    private static void sendBurst(KafkaProducer<String, AccidentEventModel> producer, String topic, int burst) {
        producer.beginTransaction();
        for (int i = 0; i < burst; i++) {
            AccidentEventModel event = EventGenerator.next();
            producer.send(new ProducerRecord<>(topic, event.getCacheId().toString(), event));
        }
        producer.commitTransaction();
    }

    private static void abortQuietly(KafkaProducer<String, AccidentEventModel> producer) {
        try {
            producer.abortTransaction();
        } catch (KafkaException e) {
            log.error("could not abort transaction: {}", e.getMessage());
        }
    }

    /** Sleeps a random 0..intervalMs between bursts; returns false if interrupted. */
    private static boolean sleepBetweenBursts(long intervalMs) {
        if (intervalMs <= 0) {
            return true;
        }
        try {
            Thread.sleep(ThreadLocalRandom.current().nextLong(intervalMs + 1));
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static KafkaProducer<String, AccidentEventModel> createProducer(BombarderConfig config) {
        String instanceId = Long.toString(ProcessHandle.current().pid());
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "stream-bombarder-" + instanceId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        // Unique transactional id per instance so several bombarders can run in parallel.
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "stream-bombarder-tx-" + UUID.randomUUID());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        // A hard-killed instance leaves an open transaction that blocks read_committed
        // consumers on those partitions until it times out — keep that window short
        // (default is 60s).
        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10_000);
        props.put("schema.registry.url", config.schemaRegistryUrl());
        return new KafkaProducer<>(props);
    }
}
