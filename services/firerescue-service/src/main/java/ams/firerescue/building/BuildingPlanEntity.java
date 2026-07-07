package ams.firerescue.building;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * A stored building plan (a real Fire Department blueprint record). Escape routes and gas-line
 * locations are kept newline-joined; {@code address_key} is the normalized (lower-cased, trimmed)
 * address used for lookup and upsert.
 */
@Table(name = "building_plans", schema = "public")
@Entity
@Getter @Setter
@NoArgsConstructor
public class BuildingPlanEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "address_key", nullable = false, unique = true, length = 255)
    private String addressKey;

    @Column(nullable = false, length = 255)
    private String address;

    @Column(nullable = false)
    private int floors;

    @Column(name = "fire_escape_routes", length = 4000)
    private String fireEscapeRoutes;

    @Column(name = "gas_line_locations", length = 4000)
    private String gasLineLocations;
}
