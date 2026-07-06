package ams.emergency.response;

import ams.data.model.UnitType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

/**
 * Response-time record for one dispatch, filled in transition by transition as
 * {@code unit.status.events} arrive.
 *
 * <p>The dispatch id is DELIBERATELY the (assigned) primary key: every status update for the
 * same dispatch must land on the same row, so the merge/upsert path is exactly what we want —
 * replays and redeliveries just rewrite the same values.
 */
@Table(name = "response_times", schema = "public")
@Entity
@Getter @Setter
@NoArgsConstructor
public class ResponseTime {

    @Id
    @Column(name = "dispatch_id", length = 36)
    private String dispatchId;

    @Column(name = "incident_id", nullable = false, length = 64)
    private String incidentId;

    @Column(name = "unit_id", length = 16)
    private String unitId;

    @Enumerated(EnumType.STRING)
    @Column(name = "unit_type", nullable = false, length = 20)
    private UnitType unitType;

    @Column(name = "dispatched_at")
    private Instant dispatchedAt;
    @Column(name = "en_route_at")
    private Instant enRouteAt;
    @Column(name = "on_scene_at")
    private Instant onSceneAt;
    @Column(name = "cleared_at")
    private Instant clearedAt;

    @Column(name = "response_seconds")
    private Long responseSeconds;

    @Column(name = "sla_breached", nullable = false)
    private boolean slaBreached;
}
