package ams.correlation.domain;

import ams.data.model.AccidentType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.UuidGenerator;

import java.time.Instant;

/**
 * A correlated real-world incident: many citizen reports of the same accident type in the same
 * ~150 m grid cell within a rolling time window merge into ONE of these instead of triggering
 * duplicate dispatches downstream.
 */
@Table(name = "incidents", schema = "public")
@Entity
@Getter @Setter
@NoArgsConstructor
public class Incident {

    public enum State { OPEN, CLOSED }

    @Id
    @UuidGenerator
    private String id;

    @Enumerated(EnumType.STRING)
    @Column(name = "accident_type", nullable = false, length = 20)
    private AccidentType accidentType;

    // Grid cell of the FIRST report (~150 m squares); candidates match cell ± 1 in each axis.
    @Column(name = "cell_lat", nullable = false)
    private int cellLat;
    @Column(name = "cell_lng", nullable = false)
    private int cellLng;

    // First report's location, kept as the incident's canonical position.
    private String address;
    private Double latitude;
    private Double longitude;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 10)
    private State state = State.OPEN;

    @Column(name = "report_count", nullable = false)
    private int reportCount;

    @Column(name = "first_reported_at", nullable = false)
    private Instant firstReportedAt;
    @Column(name = "last_reported_at", nullable = false)
    private Instant lastReportedAt;
}
