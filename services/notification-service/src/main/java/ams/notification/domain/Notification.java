package ams.notification.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

/**
 * One delivered (or suppressed) operational notification. The unique {@code dedup_key} is what
 * makes alert consumption idempotent: replays and redeliveries of the same alert are dropped.
 */
@Table(name = "notifications", schema = "public")
@Entity
@Getter @Setter
@NoArgsConstructor
public class Notification {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "dedup_key", nullable = false, length = 200, unique = true)
    private String dedupKey;

    /** BOLO / SLA / FRAUD / GEOFENCE */
    @Column(nullable = false, length = 20)
    private String source;

    /** CRITICAL / HIGH / MEDIUM / INFO */
    @Column(nullable = false, length = 10)
    private String severity;

    @Column(nullable = false, length = 200)
    private String title;

    @Column(length = 1000)
    private String message;

    @Column(name = "incident_id", length = 64)
    private String incidentId;

    /** Per-channel delivery results, e.g. {@code log:sent,webhook:failed}. */
    @Column(length = 200)
    private String channels;

    @Column(name = "rate_limited", nullable = false)
    private boolean rateLimited;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;
}
