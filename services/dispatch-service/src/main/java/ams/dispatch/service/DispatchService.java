package ams.dispatch.service;

import ams.data.model.UnitDispatchStatus;
import ams.data.model.UnitStatusEvent;
import ams.data.model.UnitType;
import ams.dispatch.domain.Dispatch;
import ams.dispatch.domain.DispatchStatus;
import ams.dispatch.domain.Unit;
import ams.dispatch.domain.UnitState;
import ams.dispatch.repository.DispatchRepository;
import ams.dispatch.repository.UnitRepository;
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
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The dispatch core: assigns the nearest AVAILABLE unit of the required type to each incident,
 * stacks calls when no unit is free, and (via the simulator schedule) advances active dispatches
 * through {@code DISPATCHED → EN_ROUTE → ON_SCENE → CLEARED}, emitting a {@link UnitStatusEvent}
 * on every transition. Events are published only after the transaction commits, so a rollback
 * can never leak a phantom status.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DispatchService {

    private static final Set<DispatchStatus> ACTIVE =
            EnumSet.of(DispatchStatus.DISPATCHED, DispatchStatus.EN_ROUTE, DispatchStatus.ON_SCENE);
    private static final double EARTH_RADIUS_M = 6_371_000;

    private final DispatchRepository dispatchRepository;
    private final UnitRepository unitRepository;
    private final KafkaTemplate<String, UnitStatusEvent> unitStatusKafkaTemplate;
    private final MeterRegistry meterRegistry;

    @Value("${kafka.topic.unit-status}")
    private String unitStatusTopic;

    /**
     * Creates (at most once per incident and unit type) a dispatch and assigns the nearest free
     * unit; with none available the dispatch queues as {@code WAITING} (call stacking).
     */
    @Transactional
    public void assign(IncidentRef incident, UnitType unitType) {
        if (dispatchRepository.existsByCacheIdAndUnitType(incident.cacheId(), unitType)) {
            return;   // redelivery — already handled
        }
        Dispatch dispatch = new Dispatch();
        dispatch.setCacheId(incident.cacheId());
        dispatch.setUnitType(unitType);
        dispatch.setAddress(incident.address());
        dispatch.setLatitude(incident.latitude());
        dispatch.setLongitude(incident.longitude());
        dispatch.setCreatedAt(Instant.now());
        // Persist first so the generated dispatch id exists before any status event is built.
        dispatchRepository.save(dispatch);

        if (!tryAssignNearestUnit(dispatch)) {
            meterRegistry.counter("ams.dispatch.queued", "unitType", unitType.name()).increment();
            log.warn("No {} available for incident {} — call stacked (WAITING)",
                    unitType, incident.cacheId());
        }
    }

    /**
     * Call stacking: retries WAITING dispatches (oldest first) as units free up. FIFO is kept
     * PER UNIT TYPE: once a type is exhausted this round we skip further calls of that type but
     * keep assigning the others — otherwise a long police backlog would starve ambulance and
     * fire calls queued behind it (head-of-line blocking).
     */
    @Transactional
    public void assignWaiting() {
        List<Dispatch> waiting = dispatchRepository.findTop50ByStatusOrderByCreatedAtAsc(DispatchStatus.WAITING);
        Set<UnitType> exhausted = EnumSet.noneOf(UnitType.class);
        for (Dispatch dispatch : waiting) {
            if (exhausted.contains(dispatch.getUnitType())) {
                continue;
            }
            if (tryAssignNearestUnit(dispatch)) {
                log.info("Stacked call {} ({}) assigned after wait", dispatch.getCacheId(), dispatch.getUnitType());
            } else {
                exhausted.add(dispatch.getUnitType());
            }
        }
    }

    /** The simulator tick: advances every active dispatch whose transition time has come. */
    @Transactional
    public void advanceDue() {
        List<Dispatch> due = dispatchRepository.findByStatusInAndNextTransitionAtBefore(ACTIVE, Instant.now());
        for (Dispatch dispatch : due) {
            advance(dispatch);
        }
    }

    private boolean tryAssignNearestUnit(Dispatch dispatch) {
        List<Unit> available = unitRepository.lockByTypeAndState(dispatch.getUnitType(), UnitState.AVAILABLE);
        Optional<Unit> nearest = available.stream().min(Comparator.comparingDouble(unit ->
                haversineMeters(unit.getLatitude(), unit.getLongitude(),
                        orZero(dispatch.getLatitude()), orZero(dispatch.getLongitude()))));
        if (nearest.isEmpty()) {
            return false;
        }
        Unit unit = nearest.get();
        unit.setState(UnitState.DISPATCHED);

        Instant now = Instant.now();
        dispatch.setUnitId(unit.getId());
        dispatch.setStatus(DispatchStatus.DISPATCHED);
        dispatch.setDispatchedAt(now);
        dispatch.setNextTransitionAt(now.plusSeconds(randomBetween(3, 8)));

        meterRegistry.counter("ams.dispatch.assigned", "unitType", dispatch.getUnitType().name()).increment();
        log.info("Dispatched {} to incident {} at '{}'", unit.getId(), dispatch.getCacheId(), dispatch.getAddress());
        publishAfterCommit(statusEvent(dispatch, UnitDispatchStatus.DISPATCHED, now));
        return true;
    }

    private void advance(Dispatch dispatch) {
        Instant now = Instant.now();
        switch (dispatch.getStatus()) {
            case DISPATCHED -> {
                dispatch.setStatus(DispatchStatus.EN_ROUTE);
                dispatch.setEnRouteAt(now);
                dispatch.setNextTransitionAt(now.plusSeconds(randomBetween(5, 15)));
                publishAfterCommit(statusEvent(dispatch, UnitDispatchStatus.EN_ROUTE, now));
            }
            case EN_ROUTE -> {
                dispatch.setStatus(DispatchStatus.ON_SCENE);
                dispatch.setOnSceneAt(now);
                dispatch.setNextTransitionAt(now.plusSeconds(randomBetween(8, 20)));
                publishAfterCommit(statusEvent(dispatch, UnitDispatchStatus.ON_SCENE, now));
            }
            case ON_SCENE -> {
                dispatch.setStatus(DispatchStatus.CLEARED);
                dispatch.setClearedAt(now);
                dispatch.setNextTransitionAt(null);
                unitRepository.findById(dispatch.getUnitId()).ifPresent(unit -> {
                    unit.setState(UnitState.AVAILABLE);
                    // the unit has physically moved: it becomes available AT the incident site
                    if (dispatch.getLatitude() != null && dispatch.getLongitude() != null) {
                        unit.setLatitude(dispatch.getLatitude());
                        unit.setLongitude(dispatch.getLongitude());
                    }
                });
                meterRegistry.counter("ams.dispatch.completed",
                        "unitType", dispatch.getUnitType().name()).increment();
                publishAfterCommit(statusEvent(dispatch, UnitDispatchStatus.CLEARED, now));
            }
            default -> { /* WAITING/CLEARED are never in the due query */ }
        }
    }

    private UnitStatusEvent statusEvent(Dispatch dispatch, UnitDispatchStatus status, Instant at) {
        return UnitStatusEvent.newBuilder()
                .setDispatchId(dispatch.getId() != null ? dispatch.getId() : "")
                .setIncidentId(dispatch.getCacheId())
                .setUnitId(dispatch.getUnitId())
                .setUnitType(dispatch.getUnitType())
                .setStatus(status)
                .setLatitude(dispatch.getLatitude() != null ? dispatch.getLatitude().toString() : "")
                .setLongitude(dispatch.getLongitude() != null ? dispatch.getLongitude().toString() : "")
                .setTimestamp(at)
                .build();
    }

    /** Publish only once the surrounding transaction commits — no phantom events on rollback. */
    private void publishAfterCommit(UnitStatusEvent event) {
        Runnable send = () -> unitStatusKafkaTemplate.send(unitStatusTopic, event.getIncidentId().toString(), event);
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

    private static long randomBetween(int minSeconds, int maxSeconds) {
        return ThreadLocalRandom.current().nextLong(minSeconds, maxSeconds + 1L);
    }

    private static double orZero(Double value) {
        return value != null ? value : 0.0;
    }

    private static double haversineMeters(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        return EARTH_RADIUS_M * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }
}
