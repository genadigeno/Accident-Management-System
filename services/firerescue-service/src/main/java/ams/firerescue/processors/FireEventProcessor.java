package ams.firerescue.processors;

import ams.data.model.FireAccidentModel;
import ams.firerescue.jpa.FireAccident;
import ams.firerescue.jpa.FireAccidentRepository;
import ams.firerescue.mapper.FireRescueMapper;
import ams.firerescue.service.FireAccidentService;
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
public class FireEventProcessor {
    private final FireAccidentService fireAccidentService;

    public void process(List<ConsumerRecord<String, FireAccidentModel>> records) {
        log.info("Total messages - {}", records.size());

        List<FireAccident> batch = new ArrayList<>();
        try {
            for (ConsumerRecord<String, FireAccidentModel> rec : records){
                log.info("Record: value - {}, key - {}", rec.value(), rec.key());
                FireAccident entity = FireRescueMapper.MAPPER.fireAccidentModelToFireAccident(rec.value());
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
            fireAccidentService.saveBatch(batch);
            log.info("Batch saved");
        }
    }

}
