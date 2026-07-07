package ams.correlation.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

/**
 * One citizen report attached to a correlated incident. The report's cacheId is DELIBERATELY
 * the (assigned) primary key: a redelivered report hits the same row, making correlation
 * idempotent (the service checks existence before processing; the PK is the race safety net).
 */
@Table(name = "incident_reports", schema = "public")
@Entity
@Getter @Setter
@NoArgsConstructor
public class IncidentReport {

    @Id
    @Column(name = "report_id", length = 64)
    private String reportId;

    @Column(name = "incident_id", nullable = false, length = 36)
    private String incidentId;

    @Column(name = "reported_at", nullable = false)
    private Instant reportedAt;
}
