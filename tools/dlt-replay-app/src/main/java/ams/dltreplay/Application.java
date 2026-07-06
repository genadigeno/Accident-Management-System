package ams.dltreplay;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Dead-letter replay tool.
 *
 * Reads records from a dead-letter topic and republishes their original key/value bytes to a
 * target topic (typically the topic the failed records came from), so they can be re-processed
 * after the underlying problem is fixed. Spring's internal DLT_* / deserialization-exception
 * headers are stripped so the replayed record re-enters the pipeline cleanly.
 *
 * Usage:
 *   java -jar dlt-replay.jar \
 *       --bootstrap.servers=localhost:9092,localhost:9093 \
 *       --source.topic=emergency.events.dlt \
 *       --target.topic=emergency.events \
 *       [--group.id=dlt-replay] [--max=0] [--dry-run=false]
 *
 *   --max=0 replays everything currently on the topic; a positive value caps the count.
 */
public class Application {

    private static final Logger log = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        Map<String, String> opts = parseArgs(args);
        String bootstrap = opts.getOrDefault("bootstrap.servers", "localhost:9092,localhost:9093");
        String sourceTopic = require(opts, "source.topic");
        String targetTopic = require(opts, "target.topic");
        String groupId = opts.getOrDefault("group.id", "dlt-replay-" + UUID.randomUUID());
        long max = Long.parseLong(opts.getOrDefault("max", "0"));
        boolean dryRun = Boolean.parseBoolean(opts.getOrDefault("dry-run", "false"));

        log.info("DLT replay: {} -> {} (bootstrap={}, max={}, dryRun={})",
                sourceTopic, targetTopic, bootstrap, max == 0 ? "all" : max, dryRun);

        long replayed = 0;
        try (KafkaConsumer<byte[], byte[]> consumer = consumer(bootstrap, groupId);
             KafkaProducer<byte[], byte[]> producer = producer(bootstrap)) {

            consumer.subscribe(List.of(sourceTopic));
            int emptyPolls = 0;
            int assignmentWaits = 0;
            while (emptyPolls < 3 && (max == 0 || replayed < max)) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(2));
                if (records.isEmpty()) {
                    // Empty polls BEFORE the group assigns partitions are just a slow rebalance,
                    // not a drained topic — counting them caused a false "0 replayed" success.
                    if (consumer.assignment().isEmpty()) {
                        if (++assignmentWaits >= 30) {
                            log.error("no partition assignment after ~60s — is the topic/broker reachable? aborting");
                            break;
                        }
                    } else {
                        emptyPolls++;
                    }
                    continue;
                }
                emptyPolls = 0;
                // Commit only the offsets of records actually republished. A bare commitSync()
                // would commit the whole poll — records consumed past --max would be silently
                // skipped (lost) on the next run.
                Map<TopicPartition, OffsetAndMetadata> replayedOffsets = new HashMap<>();
                for (ConsumerRecord<byte[], byte[]> rec : records) {
                    if (max != 0 && replayed >= max) {
                        break;
                    }
                    ProducerRecord<byte[], byte[]> out = new ProducerRecord<>(targetTopic, rec.key(), rec.value());
                    copyBusinessHeaders(rec, out);
                    if (!dryRun) {
                        producer.send(out);
                        replayedOffsets.put(new TopicPartition(rec.topic(), rec.partition()),
                                new OffsetAndMetadata(rec.offset() + 1));
                    }
                    replayed++;
                }
                if (!dryRun && !replayedOffsets.isEmpty()) {
                    producer.flush();
                    consumer.commitSync(replayedOffsets);
                }
            }
        }
        log.info("DLT replay finished: {} record(s){}", replayed, dryRun ? " (dry-run, nothing sent)" : " republished");
    }

    private static KafkaConsumer<byte[], byte[]> consumer(String bootstrap, String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return new KafkaConsumer<>(props);
    }

    private static KafkaProducer<byte[], byte[]> producer(String bootstrap) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return new KafkaProducer<>(props);
    }

    /** Copy headers except Spring's internal DLT / deserialization-exception headers. */
    private static void copyBusinessHeaders(ConsumerRecord<byte[], byte[]> rec, ProducerRecord<byte[], byte[]> out) {
        for (Header h : rec.headers()) {
            String name = h.key();
            if (name.startsWith("kafka_dlt-") || name.startsWith("springDeserializerException")) {
                continue;
            }
            out.headers().add(h);
        }
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> opts = new HashMap<>();
        for (String arg : args) {
            if (arg.startsWith("--") && arg.contains("=")) {
                String[] kv = arg.substring(2).split("=", 2);
                opts.put(kv[0], kv[1]);
            }
        }
        return opts;
    }

    private static String require(Map<String, String> opts, String key) {
        String value = opts.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required argument --" + key);
        }
        return value;
    }
}
