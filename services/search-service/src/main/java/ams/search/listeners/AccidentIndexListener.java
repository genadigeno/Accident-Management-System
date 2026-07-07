package ams.search.listeners;

import ams.data.model.AccidentEventModel;
import ams.search.service.IndexingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;

import java.util.List;

/**
 * Consumes every reported incident and indexes it into Elasticsearch. Elasticsearch being
 * unreachable or returning 5xx propagates (batch retried); a malformed / unindexable record is
 * raised per-record so only it is dead-lettered.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AccidentIndexListener {

    private final IndexingService indexingService;

    @KafkaListener(
            topics = "${kafka.source.topic}",
            containerFactory = "kafkaListenerContainerFactory",
            concurrency = "${LISTENER_CONCURRENCY:3}",
            batch = "true"
    )
    public void handle(List<ConsumerRecord<String, AccidentEventModel>> records, Acknowledgment ack) {
        log.info("Indexing {} incident(s)", records.size());
        for (ConsumerRecord<String, AccidentEventModel> rec : records) {
            // A null value means the record could not be deserialized (ErrorHandlingDeserializer).
            if (rec.value() == null) {
                throw new BatchListenerFailedException("value could not be deserialized", rec);
            }
            try {
                indexingService.index(rec.value());
            } catch (ResourceAccessException | HttpServerErrorException infra) {
                throw infra;   // Elasticsearch down / 5xx — let the error handler retry the batch
            } catch (Exception ex) {
                log.error("Poison record at {}-{}@{}: {}", rec.topic(), rec.partition(), rec.offset(),
                        ex.getMessage(), ex);
                throw new BatchListenerFailedException("record cannot be indexed", ex, rec);
            }
        }
        ack.acknowledge();
    }
}
