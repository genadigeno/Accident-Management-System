package ams.lawenforcement.service;

import ams.data.model.AlertEvent;
import ams.data.model.AlertSeverity;
import ams.lawenforcement.bolo.BoloLevel;
import ams.lawenforcement.repository.LawEnforcementAccident;
import ams.lawenforcement.repository.LawEnforcementRepository;
import ams.lawenforcement.repository.LawEnforcementRepository.PartitionOffset;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Instant;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class LawEnforcementService {
    private final LawEnforcementRepository policeAccidentRepository;
    private final MeterRegistry meterRegistry;
    private final KafkaTemplate<Object, Object> kafkaTemplate;

    @Value("${kafka.bolo-alerts.topic}")
    private String boloAlertsTopic;

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
        List<LawEnforcementAccident> persisted = dedupeByCacheId(fresh);
        policeAccidentRepository.saveAll(persisted);
        raiseBoloAlerts(persisted);
    }

    /**
     * Alerts (metric + log + {@code bolo.alerts} event for the notification service) are raised
     * here — for records that were actually persisted — and NOT during mapping: side effects in
     * the mapping loop re-fire on every batch retry, which inflated {@code ams.bolo.alerts}
     * (observed 8 alerts for 2 incidents). The event is published only after the transaction
     * commits, so a rollback can never leak a phantom alert.
     */
    private void raiseBoloAlerts(List<LawEnforcementAccident> persisted) {
        for (LawEnforcementAccident accident : persisted) {
            BoloLevel level = accident.getBoloLevel();
            if (level == null || level == BoloLevel.NONE) {
                continue;
            }
            Counter.builder("ams.bolo.alerts")
                    .tag("level", level.name())
                    .description("Number of BOLO alerts raised, by severity")
                    .register(meterRegistry)
                    .increment();
            log.warn("BOLO [{}] raised for description: \"{}\"", level, accident.getDescription());
            publishAfterCommit(boloAlert(accident, level));
        }
    }

    private AlertEvent boloAlert(LawEnforcementAccident accident, BoloLevel level) {
        String incidentId = accident.getCacheId() != null ? accident.getCacheId() : "";
        return AlertEvent.newBuilder()
                .setSource("BOLO")
                .setSeverity(level == BoloLevel.CRITICAL ? AlertSeverity.CRITICAL : AlertSeverity.HIGH)
                .setTitle("BOLO " + level + (level == BoloLevel.CRITICAL
                        ? " — notify SWAT / counter-terrorism" : " — broadcast to patrol cars"))
                .setMessage("\"" + accident.getDescription() + "\" at " + accident.getAddress())
                .setIncidentId(incidentId)
                .setDedupKey("BOLO:" + incidentId + ":" + level)
                .setTimestamp(Instant.now())
                .build();
    }

    private void publishAfterCommit(AlertEvent alert) {
        Runnable send = () -> kafkaTemplate.send(boloAlertsTopic, alert.getIncidentId().toString(), alert);
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    send.run();
                }
            });
        } else {
            send.run();
        }
    }

    /** Drops records whose identity already exists (replays) or repeats within the batch. */
    private List<LawEnforcementAccident> dedupeByCacheId(List<LawEnforcementAccident> batch) {
        List<String> ids = batch.stream()
                .map(LawEnforcementAccident::getCacheId)
                .filter(Objects::nonNull)
                .toList();
        Set<String> seen = ids.isEmpty()
                ? new HashSet<>()
                : new HashSet<>(policeAccidentRepository.findExistingCacheIds(ids));
        List<LawEnforcementAccident> result = batch.stream()
                .filter(e -> e.getCacheId() == null || seen.add(e.getCacheId()))
                .toList();
        int replays = batch.size() - result.size();
        if (replays > 0) {
            log.info("idempotency: skipped {} replayed record(s) (cacheId already persisted)", replays);
        }
        return result;
    }

    public List<LawEnforcementAccident> findTop50ByBoloLevelInOrderByIdDesc(BoloLevel... boloLevels) {
        return policeAccidentRepository.findTop50ByBoloLevelInOrderByIdDesc(boloLevels);
    }
}
