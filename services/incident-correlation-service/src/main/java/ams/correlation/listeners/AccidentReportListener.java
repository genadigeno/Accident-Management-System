package ams.correlation.listeners;

import ams.data.model.AccidentEventModel;
import ams.correlation.service.CorrelationService;
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
 * Consumes every raw report from the source topic and asks the correlation core to open a new
 * incident or merge into an existing one. Same failure policy as every consumer: poison records
 * are raised per-record; infrastructure failures retry the batch (correlation is idempotent per
 * report id).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AccidentReportListener {

    private final CorrelationService correlationService;

    @KafkaListener(
            topics = "${kafka.source.topic}",
            containerFactory = "kafkaListenerContainerFactory",
            concurrency = "${LISTENER_CONCURRENCY:3}",
            batch = "true"
    )
    public void handle(List<ConsumerRecord<String, AccidentEventModel>> records, Acknowledgment ack) {
        log.info("Reports - {}", records.size());
        for (ConsumerRecord<String, AccidentEventModel> rec : records) {
            // A null value means the record could not be deserialized (ErrorHandlingDeserializer).
            if (rec.value() == null) {
                throw new BatchListenerFailedException("value could not be deserialized", rec);
            }
            AccidentEventModel event = rec.value();
            try {
                String reportId = event.getCacheId() != null ? event.getCacheId().toString() : rec.key();
                String address = event.getLocation() != null && event.getLocation().getAddress() != null
                        ? event.getLocation().getAddress().toString() : "";
                double lat = parse(event.getLocation() != null ? event.getLocation().getLatitude() : null);
                double lng = parse(event.getLocation() != null ? event.getLocation().getLongitude() : null);
                correlationService.correlate(reportId, event.getType(), address, lat, lng);
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

    private static double parse(CharSequence value) {
        return value == null || value.isEmpty() ? 0.0 : Double.parseDouble(value.toString());
    }
}
