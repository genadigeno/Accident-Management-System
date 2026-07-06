package ams.notification.listeners;

import ams.data.model.AccidentEventModel;
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
 * Consumes geo-fenced incidents ({@code accident.events.sensitive} — full accident events at
 * sensitive addresses). One notification per incident: the cacheId is the dedup identity.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SensitiveZoneListener {

    private final NotificationService notificationService;

    @KafkaListener(
            topics = "${kafka.sensitive-alerts.topic}",
            containerFactory = "kafkaListenerContainerFactory",
            concurrency = "1",
            batch = "true"
    )
    public void handle(List<ConsumerRecord<String, AccidentEventModel>> records, Acknowledgment ack) {
        log.info("Sensitive-zone alerts - {}", records.size());
        for (ConsumerRecord<String, AccidentEventModel> rec : records) {
            if (rec.value() == null) {
                throw new BatchListenerFailedException("value could not be deserialized", rec);
            }
            AccidentEventModel event = rec.value();
            try {
                String incidentId = event.getCacheId() != null ? event.getCacheId().toString() : rec.key();
                String address = event.getLocation() != null && event.getLocation().getAddress() != null
                        ? event.getLocation().getAddress().toString() : "unknown location";
                notificationService.record(
                        "GEOFENCE",
                        "HIGH",
                        "Incident in sensitive zone",
                        event.getType() + " at " + address + ": " + event.getDescription(),
                        incidentId,
                        "GEOFENCE:" + incidentId);
            } catch (DataAccessException | TransactionException infra) {
                throw infra;
            } catch (Exception ex) {
                log.error("Poison record at {}-{}@{}: {}", rec.topic(), rec.partition(), rec.offset(),
                        ex.getMessage(), ex);
                throw new BatchListenerFailedException("record cannot be processed", ex, rec);
            }
        }
        ack.acknowledge();
    }
}
