package ams.correlation.service;

import ams.data.model.AccidentType;
import ams.data.model.IncidentEvent;
import ams.data.model.IncidentStatus;
import ams.correlation.domain.Incident;
import ams.correlation.domain.IncidentReport;
import ams.correlation.domain.IncidentReportRepository;
import ams.correlation.domain.IncidentRepository;
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
import java.util.Optional;

/**
 * Duplicate-report merging (the constructive sibling of the router's fraud detector): reports
 * of the same accident type in the same ~150 m grid-cell neighbourhood within a rolling window
 * become ONE incident with a growing report count. Every change is published to
 * {@code incident.events} (OPENED / UPDATED / CLOSED) after the transaction commits; incidents
 * with no new reports for a configurable idle period auto-close.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CorrelationService {

    /** ~150 m at mid latitudes; cell = floor(coordinate / CELL_DEGREES). */
    static final double CELL_DEGREES = 0.0015;

    private final IncidentRepository incidentRepository;
    private final IncidentReportRepository reportRepository;
    private final KafkaTemplate<Object, Object> kafkaTemplate;
    private final MeterRegistry meterRegistry;

    @Value("${kafka.incident.topic}")
    private String incidentTopic;

    @Value("${correlation.window-minutes:10}")
    private long windowMinutes;

    @Value("${correlation.close-after-minutes:30}")
    private long closeAfterMinutes;

    @Transactional
    public void correlate(String reportId, AccidentType type, String address,
                          double latitude, double longitude) {
        if (reportRepository.existsById(reportId)) {
            return;   // redelivery — already attached
        }
        Instant now = Instant.now();
        int cellLat = cell(latitude);
        int cellLng = cell(longitude);
        Instant since = now.minus(Duration.ofMinutes(windowMinutes));

        List<Incident> candidates = incidentRepository.lockCandidates(
                Incident.State.OPEN, type, cellLat - 1, cellLat + 1, cellLng - 1, cellLng + 1, since);

        Incident incident;
        IncidentStatus status;
        if (candidates.isEmpty()) {
            incident = new Incident();
            incident.setAccidentType(type);
            incident.setCellLat(cellLat);
            incident.setCellLng(cellLng);
            incident.setAddress(address);
            incident.setLatitude(latitude);
            incident.setLongitude(longitude);
            incident.setReportCount(1);
            incident.setFirstReportedAt(now);
            incident.setLastReportedAt(now);
            incidentRepository.save(incident);   // assigns the UUID
            status = IncidentStatus.OPENED;
            meterRegistry.counter("ams.incidents.opened").increment();
            log.info("Incident {} OPENED ({} at '{}')", incident.getId(), type, address);
        } else {
            incident = candidates.get(0);
            incident.setReportCount(incident.getReportCount() + 1);
            incident.setLastReportedAt(now);
            status = IncidentStatus.UPDATED;
            meterRegistry.counter("ams.incidents.merged").increment();
            log.info("Report {} MERGED into incident {} (report #{})",
                    reportId, incident.getId(), incident.getReportCount());
        }

        IncidentReport report = new IncidentReport();
        report.setReportId(reportId);
        report.setIncidentId(incident.getId());
        report.setReportedAt(now);
        reportRepository.save(report);

        publishAfterCommit(incidentEvent(incident, status, reportId, now));
    }

    /** Auto-close: an incident with no new reports for the idle period is over. */
    @Transactional
    public void closeIdleIncidents() {
        Instant cutoff = Instant.now().minus(Duration.ofMinutes(closeAfterMinutes));
        List<Incident> stale = incidentRepository.findTop50ByStateAndLastReportedAtBefore(
                Incident.State.OPEN, cutoff);
        for (Incident incident : stale) {
            incident.setState(Incident.State.CLOSED);
            meterRegistry.counter("ams.incidents.closed").increment();
            log.info("Incident {} CLOSED after {} min idle ({} report(s))",
                    incident.getId(), closeAfterMinutes, incident.getReportCount());
            publishAfterCommit(incidentEvent(incident, IncidentStatus.CLOSED, "", Instant.now()));
        }
    }

    /** The gateway's duplicate hint: the open incident a new report at this spot would join. */
    @Transactional(readOnly = true)
    public Optional<Incident> findOpenIncidentNearby(AccidentType type, double latitude, double longitude) {
        int cellLat = cell(latitude);
        int cellLng = cell(longitude);
        Instant since = Instant.now().minus(Duration.ofMinutes(windowMinutes));
        return incidentRepository.findCandidates(
                        Incident.State.OPEN, type, cellLat - 1, cellLat + 1, cellLng - 1, cellLng + 1, since)
                .stream().findFirst();
    }

    static int cell(double coordinate) {
        return (int) Math.floor(coordinate / CELL_DEGREES);
    }

    private IncidentEvent incidentEvent(Incident incident, IncidentStatus status,
                                        String lastReportId, Instant at) {
        return IncidentEvent.newBuilder()
                .setIncidentId(incident.getId())
                .setStatus(status)
                .setAccidentType(incident.getAccidentType())
                .setAddress(incident.getAddress() != null ? incident.getAddress() : "")
                .setLatitude(incident.getLatitude() != null ? incident.getLatitude().toString() : "")
                .setLongitude(incident.getLongitude() != null ? incident.getLongitude().toString() : "")
                .setReportCount(incident.getReportCount())
                .setLastReportId(lastReportId)
                .setTimestamp(at)
                .build();
    }

    /** Publish only once the surrounding transaction commits — no phantom events on rollback. */
    private void publishAfterCommit(IncidentEvent event) {
        Runnable send = () -> kafkaTemplate.send(incidentTopic, event.getIncidentId().toString(), event);
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
}
