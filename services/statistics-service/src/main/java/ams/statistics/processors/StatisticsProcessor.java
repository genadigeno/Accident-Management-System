package ams.statistics.processors;

import ams.data.model.StatisticalModel;
import ams.statistics.jpa.StatisticalModelData;
import ams.statistics.jpa.StatisticalModelRepository;
import ams.statistics.jpa.WindowedId;
import ams.statistics.service.StatisticsService;
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
public class StatisticsProcessor {
    private final StatisticsService statisticsService;

    public void process(List<ConsumerRecord<String, StatisticalModel>> records) {
        log.info("Total messages - {}", records.size());

        List<StatisticalModelData> batch = new ArrayList<>();
        try {
            for (ConsumerRecord<String, StatisticalModel> rec : records){
                log.info("Record: value - {}, key - {}", rec.value(), rec.key());
                //collect in a batch
                batch.add(StatisticalModelData.builder()
                                .id(WindowedId.builder()
                                        .end(rec.value().getEnd())
                                        .start(rec.value().getFrom())
                                        .type(rec.value().getType())
                                        .build())
                                .count(rec.value().getCount())
                        .build());
            }
        } catch (Exception ex) {
            log.error("Error processing record due to {}", ex.getMessage(), ex);
            //re-throw an exception to trigger the recoverer
            throw ex;
        } finally {
            log.info("batch size - {}", batch.size());
            //in case an exception still save a batch we got before the exception
            statisticsService.saveBatch(batch);
            log.info("batch saved");
        }
    }

}
