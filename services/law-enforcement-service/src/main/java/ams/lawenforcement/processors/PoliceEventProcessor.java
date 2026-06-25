package ams.lawenforcement.processors;

import ams.data.model.PoliceEventModel;
import ams.lawenforcement.bolo.BoloDetector;
import ams.lawenforcement.repository.LawEnforcementAccident;
import ams.lawenforcement.mapper.PoliceMapper;
import ams.lawenforcement.service.LawEnforcementService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
        try {
            for (ConsumerRecord<String, PoliceEventModel> rec : records){
                log.info("Record: value - {}, key - {}", rec.value(), rec.key());
                LawEnforcementAccident entity = PoliceMapper.MAPPER.policeEventModelToPoliceAccident(rec.value());
                entity.setKafkaTopic(rec.topic());
                entity.setKafkaPartition(rec.partition());
                entity.setKafkaOffset(rec.offset());
                entity.setBoloLevel(boloDetector.detect(
                        rec.value().getDescription() == null ? null : rec.value().getDescription().toString()));
                batch.add(entity);
            }
        } catch (Exception ex) {
            log.error("Error processing record due to {}", ex.getMessage(), ex);
            //re-throw an exception to trigger the recoverer
            throw ex;
        } finally {
            log.info("Batch size - {}", batch.size());
            //in case an exception still save a batch we got before the exception
            lawEnforcementService.saveBatch(batch);
            log.info("Batch saved");
        }

    }
}
