package ams.firerescue.jpa;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import jakarta.persistence.*;
import java.time.LocalDate;

@Table(name = "fire_accidents", schema = "public",
        uniqueConstraints = {
                @UniqueConstraint(
                        name = "uq_fire_accidents_coordinates",
                        columnNames = {"kafka_topic", "kafka_partition", "kafka_offset"}),
                @UniqueConstraint(
                        name = "uq_fire_accidents_cache_id",
                        columnNames = {"cache_id"})})
@Entity
@Getter @Setter
@NoArgsConstructor
public class FireAccident {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "fire_accidents_id_seq")
    @SequenceGenerator(name = "fire_accidents_id_seq", sequenceName = "fire_accidents_id_seq", allocationSize = 50)
    private Long id;
    private String address;
    private String latitude;
    private String longitude;
    private String description;
    @Column(name = "accident_date")
    private LocalDate date;

    // Business identity of the accident (UUID assigned at ingestion). Deduplicates replayed
    // records (whose Kafka coordinates are new) and correlates the same accident across services.
    @Column(name = "cache_id", length = 64)
    private String cacheId;

    // Kafka coordinates of the source record — used for idempotent consumption.
    @Column(name = "kafka_topic", nullable = false)
    private String kafkaTopic;
    @Column(name = "kafka_partition", nullable = false)
    private int kafkaPartition;
    @Column(name = "kafka_offset", nullable = false)
    private long kafkaOffset;
}
