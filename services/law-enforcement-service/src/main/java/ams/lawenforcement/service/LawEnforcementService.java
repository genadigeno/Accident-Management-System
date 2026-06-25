package ams.lawenforcement.service;

import ams.lawenforcement.bolo.BoloLevel;
import ams.lawenforcement.repository.LawEnforcementAccident;
import ams.lawenforcement.repository.LawEnforcementRepository;
import ams.lawenforcement.repository.LawEnforcementRepository.PartitionOffset;
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
public class LawEnforcementService {
    private final LawEnforcementRepository policeAccidentRepository;

    /**
     * Persists the batch idempotently: records whose Kafka offset is at or below the
     * highest already-persisted offset for their partition are skipped, so re-delivered
     * records never create duplicates. A unique index on the Kafka coordinates is the
     * safety net against any race.
     */
    @Transactional(rollbackFor = Exception.class)
    public void saveBatch(List<LawEnforcementAccident> batch) {
        if (batch.isEmpty()) {
            return;
        }
        String topic = batch.get(0).getKafkaTopic();
        Map<Integer, Long> highWaterMarks = policeAccidentRepository.findHighWaterMarks(topic).stream()
                .collect(Collectors.toMap(PartitionOffset::getPartition, PartitionOffset::getMaxOffset));

        List<LawEnforcementAccident> fresh = batch.stream()
                .filter(e -> e.getKafkaOffset() > highWaterMarks.getOrDefault(e.getKafkaPartition(), -1L))
                .collect(Collectors.toList());

        int skipped = batch.size() - fresh.size();
        if (skipped > 0) {
            log.info("idempotency: skipped {} already-consumed record(s)", skipped);
        }
        policeAccidentRepository.saveAll(fresh);
    }

    public List<LawEnforcementAccident> findTop50ByBoloLevelInOrderByIdDesc(BoloLevel... boloLevels) {
        return policeAccidentRepository.findTop50ByBoloLevelInOrderByIdDesc(boloLevels);
    }
}
