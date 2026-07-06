package ams.emergency.listeners;

import ams.data.model.UnitStatusEvent;
import ams.emergency.response.ResponseTimeService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Consumes unit lifecycle updates from the dispatch service and feeds the response-time /
 * SLA tracker. Same failure policy as the main listener: poison records are raised per-record
 * (retried briefly, then dead-lettered individually), infrastructure failures retry the batch.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class UnitStatusEventListener {

    private final ResponseTimeService responseTimeService;

    @KafkaListener(
            topics = "${kafka.unit-status.topic}",
            containerFactory = "kafkaListenerContainerFactory",
            concurrency = "1",
            batch = "true"
    )
    public void handle(List<ConsumerRecord<String, UnitStatusEvent>> records, Acknowledgment ack) {
        log.info("Unit status updates - {}", records.size());
        for (ConsumerRecord<String, UnitStatusEvent> rec : records) {
            // A null value means the record could not be deserialized (ErrorHandlingDeserializer).
            if (rec.value() == null) {
                throw new BatchListenerFailedException("value could not be deserialized", rec);
            }
            try {
                responseTimeService.apply(rec.value());
            } catch (org.springframework.dao.DataAccessException | org.springframework.transaction.TransactionException infra) {
                throw infra;   // infrastructure — let the error handler retry the batch
            } catch (Exception ex) {
                log.error("Poison record at {}-{}@{}: {}", rec.topic(), rec.partition(), rec.offset(),
                        ex.getMessage(), ex);
                throw new BatchListenerFailedException("record cannot be processed", ex, rec);
            }
        }
        ack.acknowledge();
    }
}
