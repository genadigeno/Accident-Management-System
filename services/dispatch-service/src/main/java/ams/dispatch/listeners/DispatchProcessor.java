package ams.dispatch.listeners;

import ams.data.model.UnitType;
import ams.dispatch.service.DispatchService;
import ams.dispatch.service.IncidentRef;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.function.Function;

/**
 * Shared batch-processing template for the three responder-topic listeners: maps each record to
 * an {@link IncidentRef} and asks the dispatch service for a unit of the required type.
 *
 * <p>Per-record failures (null value = deserialization failure, or unmappable coordinates) are
 * raised as {@link BatchListenerFailedException} so the error handler retries/dead-letters ONLY
 * that record; batch-wide infrastructure failures propagate as-is and are retried (dispatch
 * creation is idempotent per incident and unit type).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DispatchProcessor {

    private final DispatchService dispatchService;

    public <V> void process(List<ConsumerRecord<String, V>> records,
                            UnitType unitType,
                            Function<V, IncidentRef> mapper) {
        log.info("Total messages - {} (for {})", records.size(), unitType);
        for (ConsumerRecord<String, V> rec : records) {
            // A null value means the record could not be deserialized (ErrorHandlingDeserializer).
            if (rec.value() == null) {
                throw new BatchListenerFailedException("value could not be deserialized", rec);
            }
            IncidentRef incident;
            try {
                incident = mapper.apply(rec.value());
            } catch (Exception ex) {
                log.error("Poison record at {}-{}@{}: {}", rec.topic(), rec.partition(), rec.offset(),
                        ex.getMessage(), ex);
                throw new BatchListenerFailedException("record cannot be processed", ex, rec);
            }
            dispatchService.assign(incident, unitType);
        }
    }

    /** Common field extraction for the three responder models (same shape, no shared interface). */
    static IncidentRef toIncident(CharSequence cacheId, CharSequence address,
                                  CharSequence latitude, CharSequence longitude) {
        return new IncidentRef(
                cacheId == null ? null : cacheId.toString(),
                address == null ? "" : address.toString(),
                parseCoordinate(latitude),
                parseCoordinate(longitude));
    }

    private static double parseCoordinate(CharSequence value) {
        return value == null || value.isEmpty() ? 0.0 : Double.parseDouble(value.toString());
    }
}
