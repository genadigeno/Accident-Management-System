package ams.emergency.processors;

import ams.data.model.EmergencyEventModel;
import ams.emergency.jpa.EmergencyAccident;
import ams.emergency.jpa.EmergencyAccidentRepository;
import ams.emergency.mapper.EmergencyMapper;
import ams.emergency.service.EmergencyAccidentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
public class EmergencyEventProcessor {
    private final EmergencyAccidentService emergencyAccidentService;

    public void process(List<ConsumerRecord<String, EmergencyEventModel>> records) {
        log.info("Total messages - {}", records.size());

        List<EmergencyAccident> batch = new ArrayList<>();
        for (ConsumerRecord<String, EmergencyEventModel> rec : records) {
            // A null value means the record could not be deserialized (ErrorHandlingDeserializer).
            if (rec.value() == null) {
                emergencyAccidentService.saveBatch(batch);
                throw new BatchListenerFailedException("value could not be deserialized", rec);
            }
            try {
                log.info("Record: value - {}, key - {}", rec.value(), rec.key());
                EmergencyAccident entity = EmergencyMapper.MAPPER.emergencyEventModelToEmergencyAccident(rec.value());
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
                emergencyAccidentService.saveBatch(batch);
                throw new BatchListenerFailedException("record cannot be processed", ex, rec);
            }
        }
        log.info("Batch size - {}", batch.size());
        emergencyAccidentService.saveBatch(batch);
        log.info("Batch saved");
    }

}
