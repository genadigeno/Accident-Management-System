package ams.firerescue.building;

import java.util.Optional;

/**
 * Adapter for a building-plan backend. The default implementation is a stub; a real
 * Fire Department blueprint database can be swapped in without touching the service or
 * controller.
 */
public interface BuildingPlanProvider {
    Optional<BuildingPlan> findByAddress(String address);
}
