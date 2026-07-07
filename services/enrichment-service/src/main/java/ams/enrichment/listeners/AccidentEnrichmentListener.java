package ams.enrichment.listeners;

import ams.data.model.AccidentEventModel;
import ams.enrichment.service.EnrichmentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Consumes the live incident stream and publishes an enriched copy. Reads from {@code latest}
 * (enrichment is forward-looking — it augments new events, not replays history), so it never
 * floods the weather API with a backlog.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AccidentEnrichmentListener {

    private final EnrichmentService enrichmentService;

    @KafkaListener(
            topics = "${kafka.source.topic}",
            containerFactory = "kafkaListenerContainerFactory",
            concurrency = "${LISTENER_CONCURRENCY:3}",
            batch = "true"
    )
    public void handle(List<ConsumerRecord<String, AccidentEventModel>> records, Acknowledgment ack) {
        log.info("Enriching {} incident(s)", records.size());
        for (ConsumerRecord<String, AccidentEventModel> rec : records) {
            // A null value means the record could not be deserialized (ErrorHandlingDeserializer).
            if (rec.value() == null) {
                throw new BatchListenerFailedException("value could not be deserialized", rec);
            }
            try {
                enrichmentService.enrichAndPublish(rec.value());
            } catch (Exception ex) {
                log.error("Poison record at {}-{}@{}: {}", rec.topic(), rec.partition(), rec.offset(),
                        ex.getMessage(), ex);
                throw new BatchListenerFailedException("record cannot be enriched", ex, rec);
            }
        }
        ack.acknowledge();
    }
}
