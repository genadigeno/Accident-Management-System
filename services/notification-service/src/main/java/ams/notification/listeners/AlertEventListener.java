package ams.notification.listeners;

import ams.data.model.AlertEvent;
import ams.notification.service.NotificationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.TransactionException;

import java.util.List;

/**
 * Consumes STRUCTURED alerts ({@link AlertEvent}) — currently BOLO alerts from law-enforcement
 * and SLA breaches from emergency. The producer supplies the dedup key.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AlertEventListener {

    private final NotificationService notificationService;

    @KafkaListener(
            topics = {"${kafka.bolo-alerts.topic}", "${kafka.sla-alerts.topic}"},
            containerFactory = "kafkaListenerContainerFactory",
            concurrency = "${LISTENER_CONCURRENCY:3}",
            batch = "true"
    )
    public void handle(List<ConsumerRecord<String, AlertEvent>> records, Acknowledgment ack) {
        log.info("Structured alerts - {}", records.size());
        for (ConsumerRecord<String, AlertEvent> rec : records) {
            // A null value means the record could not be deserialized (ErrorHandlingDeserializer).
            if (rec.value() == null) {
                throw new BatchListenerFailedException("value could not be deserialized", rec);
            }
            AlertEvent alert = rec.value();
            try {
                notificationService.record(
                        alert.getSource().toString(),
                        alert.getSeverity().name(),
                        alert.getTitle().toString(),
                        alert.getMessage().toString(),
                        alert.getIncidentId().toString(),
                        alert.getDedupKey().toString());
            } catch (DataAccessException | TransactionException infra) {
                throw infra;   // infrastructure — the error handler retries the batch
            } catch (Exception ex) {
                log.error("Poison record at {}-{}@{}: {}", rec.topic(), rec.partition(), rec.offset(),
                        ex.getMessage(), ex);
                throw new BatchListenerFailedException("record cannot be processed", ex, rec);
            }
        }
        ack.acknowledge();
    }
}
