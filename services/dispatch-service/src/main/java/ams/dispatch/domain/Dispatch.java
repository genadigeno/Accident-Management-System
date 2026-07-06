package ams.dispatch.domain;

import ams.data.model.UnitType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.UuidGenerator;

import java.time.Instant;

/**
 * One unit assignment for one incident. An incident that needs several responder types (e.g. a
 * fire needs police + ambulance + engine) gets one dispatch per type — enforced idempotent by
 * the unique (cache_id, unit_type) constraint, so event redeliveries never dispatch twice.
 */
@Table(name = "dispatches", schema = "public",
        uniqueConstraints = @UniqueConstraint(
                name = "uq_dispatches_incident_unit_type",
                columnNames = {"cache_id", "unit_type"}))
@Entity
@Getter @Setter
@NoArgsConstructor
public class Dispatch {

    @Id
    @UuidGenerator
    private String id;

    // Business identity of the accident (the event's cacheId).
    @Column(name = "cache_id", nullable = false, length = 64)
    private String cacheId;

    @Enumerated(EnumType.STRING)
    @Column(name = "unit_type", nullable = false, length = 20)
    private UnitType unitType;

    @Column(name = "unit_id", length = 16)
    private String unitId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private DispatchStatus status = DispatchStatus.WAITING;

    private String address;
    private Double latitude;
    private Double longitude;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;
    @Column(name = "dispatched_at")
    private Instant dispatchedAt;
    @Column(name = "en_route_at")
    private Instant enRouteAt;
    @Column(name = "on_scene_at")
    private Instant onSceneAt;
    @Column(name = "cleared_at")
    private Instant clearedAt;

    // When the simulator should advance this dispatch to its next status.
    @Column(name = "next_transition_at")
    private Instant nextTransitionAt;
}
