package ams.event.stream.handler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;

import java.nio.charset.StandardCharsets;

/**
 * Sends records the streams topology could not deserialize to the dead-letter topic with their
 * ORIGINAL key/value bytes preserved — byte-for-byte, so dlt-replay can re-inject them exactly —
 * plus origin and exception headers for diagnosis.
 *
 * <p>(The previous version published a structured Avro error record built from
 * {@code value().toString()}; for raw bytes that is just {@code "[B@..."} — the actual payload
 * was lost and the record could never be repaired or replayed.)
 */
@Slf4j
@RequiredArgsConstructor
public class AccidentDeserializationErrorRecoverer implements ConsumerRecordRecoverer {

    private final KafkaTemplate<byte[], byte[]> dltTemplate;
    private final String dltTopic;

    @Override
    public void accept(ConsumerRecord<?, ?> record, Exception exception) {
        log.warn("Undeserializable record at {}-{}@{} -> {} ({})",
                record.topic(), record.partition(), record.offset(), dltTopic, exception.getMessage());

        ProducerRecord<byte[], byte[]> out = new ProducerRecord<>(
                dltTopic, null, asBytes(record.key()), asBytes(record.value()));
        out.headers()
                .add(new RecordHeader("dlt-origin-topic", bytes(record.topic())))
                .add(new RecordHeader("dlt-origin-partition", bytes(Integer.toString(record.partition()))))
                .add(new RecordHeader("dlt-origin-offset", bytes(Long.toString(record.offset()))))
                .add(new RecordHeader("dlt-exception", bytes(String.valueOf(exception))));
        dltTemplate.send(out);
    }

    private static byte[] asBytes(Object payload) {
        if (payload == null) {
            return null;
        }
        if (payload instanceof byte[] raw) {
            return raw;
        }
        return bytes(payload.toString());
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }
}
