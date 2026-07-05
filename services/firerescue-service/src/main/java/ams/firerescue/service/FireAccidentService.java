package ams.firerescue.service;

import ams.firerescue.jpa.FireAccident;
import ams.firerescue.jpa.FireAccidentRepository;
import ams.firerescue.jpa.FireAccidentRepository.PartitionOffset;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class FireAccidentService {
    private final FireAccidentRepository fireAccidentRepository;

    /**
     * Persists the batch idempotently, in two layers:
     * <ol>
     *   <li>records whose Kafka offset is at or below the highest already-persisted offset for
     *       their partition are skipped (plain re-deliveries);</li>
     *   <li>records whose {@code cacheId} was already persisted are skipped — replays (e.g.
     *       re-published from the DLT) arrive with NEW Kafka coordinates, so only the business
     *       identity can catch them.</li>
     * </ol>
     * Unique indexes on the Kafka coordinates and on {@code cache_id} are the safety net
     * against any race.
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
        fireAccidentRepository.saveAll(dedupeByCacheId(fresh));
    }

    /** Drops records whose identity already exists (replays) or repeats within the batch. */
    private List<FireAccident> dedupeByCacheId(List<FireAccident> batch) {
        List<String> ids = batch.stream()
                .map(FireAccident::getCacheId)
                .filter(Objects::nonNull)
                .toList();
        Set<String> seen = ids.isEmpty()
                ? new HashSet<>()
                : new HashSet<>(fireAccidentRepository.findExistingCacheIds(ids));
        List<FireAccident> result = batch.stream()
                .filter(e -> e.getCacheId() == null || seen.add(e.getCacheId()))
                .toList();
        int replays = batch.size() - result.size();
        if (replays > 0) {
            log.info("idempotency: skipped {} replayed record(s) (cacheId already persisted)", replays);
        }
        return result;
    }
}
