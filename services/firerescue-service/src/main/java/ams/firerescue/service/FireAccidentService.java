package ams.firerescue.service;

import ams.firerescue.jpa.FireAccident;
import ams.firerescue.jpa.FireAccidentRepository;
import ams.firerescue.jpa.FireAccidentRepository.PartitionOffset;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class FireAccidentService {
    private final FireAccidentRepository fireAccidentRepository;

    /**
     * Persists the batch idempotently: records whose Kafka offset is at or below the
     * highest already-persisted offset for their partition are skipped, so re-delivered
     * records never create duplicates. A unique index on the Kafka coordinates is the
     * safety net against any race.
     */
    @Transactional(rollbackFor = Exception.class)
    public void saveBatch(List<FireAccident> batch) {
        if (batch.isEmpty()) {
            return;
        }
        String topic = batch.get(0).getKafkaTopic();
        Map<Integer, Long> highWaterMarks = fireAccidentRepository.findHighWaterMarks(topic).stream()
                .collect(Collectors.toMap(PartitionOffset::getPartition, PartitionOffset::getMaxOffset));

        List<FireAccident> fresh = batch.stream()
                .filter(fireAccident ->
                        fireAccident.getKafkaOffset() > highWaterMarks.getOrDefault(
                                fireAccident.getKafkaPartition(), -1L)
                )
                .toList();

        int skipped = batch.size() - fresh.size();
        if (skipped > 0) {
            log.info("idempotency: skipped {} already-consumed record(s)", skipped);
        }
        fireAccidentRepository.saveAll(fresh);
    }
}
