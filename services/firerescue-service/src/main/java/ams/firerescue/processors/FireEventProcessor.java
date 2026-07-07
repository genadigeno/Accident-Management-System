package ams.firerescue.processors;

import ams.data.model.FireAccidentModel;
import ams.firerescue.jpa.FireAccident;
import ams.firerescue.jpa.FireAccidentRepository;
import ams.firerescue.mapper.FireRescueMapper;
import ams.firerescue.service.FireAccidentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class FireEventProcessor {
    private final FireAccidentService fireAccidentService;

    private final MeterRegistry meterRegistry;

    public void process(List<ConsumerRecord<String, FireAccidentModel>> records) {
        log.info("Total messages - {}", records.size());
        meterRegistry.counter("ams.events.received").increment(records.size());

        List<FireAccident> batch = new ArrayList<>();
        for (ConsumerRecord<String, FireAccidentModel> rec : records) {
            // A null value means the record could not be deserialized (ErrorHandlingDeserializer).
            if (rec.value() == null) {
                fireAccidentService.saveBatch(batch);
                throw new BatchListenerFailedException("value could not be deserialized", rec);
            }
            try {
                // DEBUG: per-record logging at INFO was the single biggest I/O cost under load.
                log.debug("Record: value - {}, key - {}", rec.value(), rec.key());
                FireAccident entity = FireRescueMapper.MAPPER.fireAccidentModelToFireAccident(rec.value());
                entity.setKafkaTopic(rec.topic());
                entity.setKafkaPartition(rec.partition());
                entity.setKafkaOffset(rec.offset());
                batch.add(entity);
            } catch (Exception ex) {
                log.error("Poison record at {}-{}@{}: {}", rec.topic(), rec.partition(), rec.offset(),
                        ex.getMessage(), ex);
                // Save what mapped cleanly, then point the error handler at the exact record: it
                // retries/dead-letters ONLY this record and resumes after it, instead of retrying
                // and dead-lettering the entire batch.
                fireAccidentService.saveBatch(batch);
                throw new BatchListenerFailedException("record cannot be processed", ex, rec);
            }
        }
        log.info("Batch size - {}", batch.size());
        fireAccidentService.saveBatch(batch);
        meterRegistry.counter("ams.events.processed").increment(batch.size());
        log.info("Batch saved");
    }

}
