package ams.dispatch.domain;

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

/**
 * A response unit (patrol car, ambulance, fire engine). The fleet is seeded by Flyway; a unit's
 * position updates to the incident location when it clears a call — it "moves" through the city.
 */
@Table(name = "units", schema = "public")
@Entity
@Getter @Setter
@NoArgsConstructor
public class Unit {

    @Id
    private String id;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private UnitType type;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private UnitState state = UnitState.AVAILABLE;

    @Column(nullable = false)
    private double latitude;

    @Column(nullable = false)
    private double longitude;
}
