package ams.emergency.response;

import ams.data.model.AlertEvent;
import ams.data.model.AlertSeverity;
import ams.data.model.UnitStatusEvent;
import ams.emergency.response.ResponseTimeRepository.UnitTypeStats;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Tracks how long units take to reach incidents (your spec's "EMS arrived in 8 minutes"):
 * consumes {@code unit.status.events}, upserts one row per dispatch, computes
 * dispatched→on-scene on arrival, and raises an alert + metric when the response exceeds
 * the SLA ({@code response.sla-seconds}, default 15 minutes).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ResponseTimeService {

    private final ResponseTimeRepository repository;
    private final MeterRegistry meterRegistry;
    private final KafkaTemplate<Object, Object> kafkaTemplate;

    @Value("${response.sla-seconds:900}")
    private long slaSeconds;

    @Value("${kafka.sla-alerts.topic}")
    private String slaAlertsTopic;

    @Transactional
    public void apply(UnitStatusEvent event) {
        ResponseTime row = repository.findById(event.getDispatchId().toString())
                .orElseGet(() -> newRow(event));

        Instant at = event.getTimestamp();
        switch (event.getStatus()) {
            case DISPATCHED -> row.setDispatchedAt(at);
            case EN_ROUTE -> row.setEnRouteAt(at);
            case ON_SCENE -> {
                row.setOnSceneAt(at);
                computeResponse(row);
            }
            case CLEARED -> row.setClearedAt(at);
        }
        repository.save(row);
    }

    private void computeResponse(ResponseTime row) {
        if (row.getDispatchedAt() == null || row.getOnSceneAt() == null) {
            return;   // out-of-order or lost DISPATCHED event; leave uncomputed
        }
        long seconds = Duration.between(row.getDispatchedAt(), row.getOnSceneAt()).getSeconds();
        row.setResponseSeconds(seconds);
        meterRegistry.timer("ams.response.time", "unitType", row.getUnitType().name())
                .record(Duration.ofSeconds(seconds));

        if (seconds > slaSeconds) {
            row.setSlaBreached(true);
            meterRegistry.counter("ams.sla.breached", "unitType", row.getUnitType().name()).increment();
            log.warn("SLA BREACH: {} took {}s (> {}s) to reach incident {} at dispatch {}",
                    row.getUnitId(), seconds, slaSeconds, row.getIncidentId(), row.getDispatchId());
            publishAfterCommit(slaAlert(row, seconds));
        }
    }

    private AlertEvent slaAlert(ResponseTime row, long seconds) {
        return AlertEvent.newBuilder()
                .setSource("SLA")
                // twice over the SLA is an operational emergency of its own
                .setSeverity(seconds > slaSeconds * 2 ? AlertSeverity.CRITICAL : AlertSeverity.HIGH)
                .setTitle("Response SLA breached (" + row.getUnitType() + ")")
                .setMessage("Unit " + row.getUnitId() + " took " + seconds + "s (limit " + slaSeconds
                        + "s) to reach incident " + row.getIncidentId())
                .setIncidentId(row.getIncidentId())
                .setDedupKey("SLA:" + row.getDispatchId())
                .setTimestamp(Instant.now())
                .build();
    }

    /** Publish only once the surrounding transaction commits — no phantom alerts on rollback. */
    private void publishAfterCommit(AlertEvent alert) {
        Runnable send = () -> kafkaTemplate.send(slaAlertsTopic, alert.getIncidentId().toString(), alert);
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

    private static ResponseTime newRow(UnitStatusEvent event) {
        ResponseTime row = new ResponseTime();
        row.setDispatchId(event.getDispatchId().toString());
        row.setIncidentId(event.getIncidentId().toString());
        row.setUnitId(event.getUnitId() != null ? event.getUnitId().toString() : null);
        row.setUnitType(event.getUnitType());
        return row;
    }

    @Transactional(readOnly = true)
    public List<ResponseTime> recent() {
        return repository.findTop50ByOrderByDispatchedAtDesc();
    }

    @Transactional(readOnly = true)
    public Summary summary() {
        List<UnitTypeStats> rows = repository.statsByUnitType();
        long total = 0;
        long completed = 0;
        long sumSeconds = 0;
        long breached = 0;
        for (UnitTypeStats r : rows) {
            total += r.getTotal();
            completed += r.getCompleted();
            sumSeconds += r.getSumSeconds();
            breached += r.getBreached();
        }
        Double overallAvg = completed == 0 ? null : (double) sumSeconds / completed;
        List<TypeStats> byType = rows.stream()
                .map(r -> new TypeStats(r.getUnitType(), r.getTotal(), r.getCompleted(),
                        r.getAvgSeconds(), r.getMinSeconds(), r.getMaxSeconds(), r.getBreached()))
                .toList();
        return new Summary(slaSeconds, total, completed, overallAvg, breached, byType);
    }

    public record TypeStats(String unitType, long total, long completed, Double avgSeconds,
                            Long minSeconds, Long maxSeconds, long breached) {}

    public record Summary(long slaSeconds, long totalDispatches, long completedResponses,
                          Double avgResponseSeconds, long slaBreaches, List<TypeStats> byUnitType) {}
}
