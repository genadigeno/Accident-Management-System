package ams.lawenforcement.processors;

import ams.data.model.PoliceEventModel;
import ams.lawenforcement.bolo.BoloDetector;
import ams.lawenforcement.repository.LawEnforcementAccident;
import ams.lawenforcement.mapper.PoliceMapper;
import ams.lawenforcement.service.LawEnforcementService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class PoliceEventProcessor {
    private final LawEnforcementService lawEnforcementService;
    private final BoloDetector boloDetector;

    public void process(List<ConsumerRecord<String, PoliceEventModel>> records) {
        log.info("Total messages - {}", records.size());

        List<LawEnforcementAccident> batch = new ArrayList<>();
        for (ConsumerRecord<String, PoliceEventModel> rec : records) {
            // A null value means the record could not be deserialized (ErrorHandlingDeserializer).
            if (rec.value() == null) {
                lawEnforcementService.saveBatch(batch);
                throw new BatchListenerFailedException("value could not be deserialized", rec);
            }
            try {
                // DEBUG: per-record logging at INFO was the single biggest I/O cost under load.
                log.debug("Record: value - {}, key - {}", rec.value(), rec.key());
                LawEnforcementAccident entity = PoliceMapper.MAPPER.policeEventModelToPoliceAccident(rec.value());
                entity.setKafkaTopic(rec.topic());
                entity.setKafkaPartition(rec.partition());
                entity.setKafkaOffset(rec.offset());
                entity.setBoloLevel(boloDetector.detect(
                        rec.value().getDescription() == null ? null : rec.value().getDescription().toString()));
                batch.add(entity);
            } catch (Exception ex) {
                log.error("Poison record at {}-{}@{}: {}", rec.topic(), rec.partition(), rec.offset(),
                        ex.getMessage(), ex);
                // Save what mapped cleanly, then point the error handler at the exact record: it
                // retries/dead-letters ONLY this record and resumes after it, instead of retrying
                // and dead-lettering the entire batch.
                lawEnforcementService.saveBatch(batch);
                throw new BatchListenerFailedException("record cannot be processed", ex, rec);
            }
        }
        log.info("Batch size - {}", batch.size());
        lawEnforcementService.saveBatch(batch);
        log.info("Batch saved");
    }
}
