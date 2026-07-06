package ams.notification.listeners;

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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;

/**
 * Consumes the router's rapid-repeat fraud alerts ({@code accident.events.fraud} — Avro string
 * messages, one per hot (location, window)). The message text is the alert's identity, so its
 * hash is the dedup key.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FraudAlertListener {

    private final NotificationService notificationService;

    @KafkaListener(
            topics = "${kafka.fraud-alerts.topic}",
            containerFactory = "kafkaListenerContainerFactory",
            concurrency = "1",
            batch = "true"
    )
    public void handle(List<ConsumerRecord<String, Object>> records, Acknowledgment ack) {
        log.info("Fraud alerts - {}", records.size());
        for (ConsumerRecord<String, Object> rec : records) {
            if (rec.value() == null) {
                throw new BatchListenerFailedException("value could not be deserialized", rec);
            }
            String message = rec.value().toString();
            try {
                notificationService.record(
                        "FRAUD",
                        "HIGH",
                        "Possible fraud: rapid repeat reports",
                        message,
                        null,
                        "FRAUD:" + sha256Prefix(message));
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

    private static String sha256Prefix(String text) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest, 0, 16);
        } catch (Exception e) {
            // SHA-256 is guaranteed present; fall back defensively anyway
            return Integer.toHexString(text.hashCode());
        }
    }
}
