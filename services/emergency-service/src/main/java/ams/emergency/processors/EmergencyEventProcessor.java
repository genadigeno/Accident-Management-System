package ams.emergency.processors;

import ams.data.model.EmergencyEventModel;
import ams.emergency.jpa.EmergencyAccident;
import ams.emergency.jpa.EmergencyAccidentRepository;
import ams.emergency.mapper.EmergencyMapper;
import ams.emergency.service.EmergencyAccidentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
        try {
            for (ConsumerRecord<String, EmergencyEventModel> rec : records){
                log.info("Record: value - {}, key - {}", rec.value(), rec.key());
                EmergencyAccident entity = EmergencyMapper.MAPPER.emergencyEventModelToEmergencyAccident(rec.value());
                entity.setKafkaTopic(rec.topic());
                entity.setKafkaPartition(rec.partition());
                entity.setKafkaOffset(rec.offset());
                batch.add(entity);
            }
        } catch (Exception ex) {
            log.error("Error processing record due to {}", ex.getMessage(), ex);
            //re-throw an exception to trigger the recoverer
            throw ex;
        } finally {
            log.info("Batch size - {}", batch.size());
            //in case an exception still save a batch we got before the exception
            emergencyAccidentService.saveBatch(batch);
            log.info("Batch saved");
        }
    }

}
