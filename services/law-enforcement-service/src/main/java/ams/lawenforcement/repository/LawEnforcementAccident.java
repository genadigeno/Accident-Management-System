package ams.lawenforcement.repository;

import ams.lawenforcement.bolo.BoloLevel;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDate;

@Table(name = "law_enforcement_accidents", schema = "public",
        uniqueConstraints = @UniqueConstraint(
                name = "uq_law_enforcement_accidents_coordinates",
                columnNames = {"kafka_topic", "kafka_partition", "kafka_offset"}))
@Entity
@Getter @Setter
@NoArgsConstructor
public class LawEnforcementAccident {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "law_enforcement_accidents_id_seq")
    @SequenceGenerator(name = "law_enforcement_accidents_id_seq", sequenceName = "law_enforcement_accidents_id_seq", allocationSize = 50)
    private Long id;
    private String address;
    private String latitude;
    private String longitude;
    private String description;
    @Column(name = "accident_date")
    private LocalDate date;

    // Kafka coordinates of the source record — used for idempotent consumption.
    @Column(name = "kafka_topic", nullable = false)
    private String kafkaTopic;
    @Column(name = "kafka_partition", nullable = false)
    private int kafkaPartition;
    @Column(name = "kafka_offset", nullable = false)
    private long kafkaOffset;

    // BOLO ("Be On the Lookout") severity derived from the incident description.
    @Enumerated(EnumType.STRING)
    @Column(name = "bolo_level", nullable = false, length = 20)
    private BoloLevel boloLevel = BoloLevel.NONE;
}
