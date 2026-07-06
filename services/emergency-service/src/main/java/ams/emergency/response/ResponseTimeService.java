package ams.emergency.response;

import ams.data.model.UnitStatusEvent;
import ams.emergency.response.ResponseTimeRepository.UnitTypeStats;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

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

    @Value("${response.sla-seconds:900}")
    private long slaSeconds;

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
