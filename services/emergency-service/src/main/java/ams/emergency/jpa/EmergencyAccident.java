package ams.emergency.jpa;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import jakarta.persistence.*;
import java.time.LocalDate;

@Table(name = "emergency_accidents", schema = "public",
        uniqueConstraints = @UniqueConstraint(
                name = "uq_emergency_accidents_coordinates",
                columnNames = {"kafka_topic", "kafka_partition", "kafka_offset"}))
@Entity
@Getter @Setter
@NoArgsConstructor
public class EmergencyAccident {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "emergency_accidents_id_seq")
    @SequenceGenerator(name = "emergency_accidents_id_seq", sequenceName = "emergency_accidents_id_seq", allocationSize = 50)
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
}
