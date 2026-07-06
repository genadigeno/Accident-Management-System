package ams.statistics.processors;

import ams.data.model.StatisticalModel;
import ams.statistics.jpa.StatisticalModelData;
import ams.statistics.jpa.StatisticalModelRepository;
import ams.statistics.jpa.WindowedId;
import ams.statistics.service.StatisticsService;
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
public class StatisticsProcessor {
    private final StatisticsService statisticsService;

    public void process(List<ConsumerRecord<String, StatisticalModel>> records) {
        log.info("Total messages - {}", records.size());

        List<StatisticalModelData> batch = new ArrayList<>();
        for (ConsumerRecord<String, StatisticalModel> rec : records) {
            // A null value means the record could not be deserialized (ErrorHandlingDeserializer).
            if (rec.value() == null) {
                statisticsService.saveBatch(batch);
                throw new BatchListenerFailedException("value could not be deserialized", rec);
            }
            try {
                // DEBUG: per-record logging at INFO was the single biggest I/O cost under load.
                log.debug("Record: value - {}, key - {}", rec.value(), rec.key());
                //collect in a batch
                batch.add(StatisticalModelData.builder()
                                .id(WindowedId.builder()
                                        .end(rec.value().getEnd())
                                        .start(rec.value().getFrom())
                                        .type(rec.value().getType())
                                        .build())
                                .count(rec.value().getCount())
                        .build());
            } catch (Exception ex) {
                log.error("Poison record at {}-{}@{}: {}", rec.topic(), rec.partition(), rec.offset(),
                        ex.getMessage(), ex);
                // Save what mapped cleanly, then point the error handler at the exact record: it
                // retries/dead-letters ONLY this record and resumes after it, instead of retrying
                // and dead-lettering the entire batch.
                statisticsService.saveBatch(batch);
                throw new BatchListenerFailedException("record cannot be processed", ex, rec);
            }
        }
        log.info("batch size - {}", batch.size());
        statisticsService.saveBatch(batch);
        log.info("batch saved");
    }

}
