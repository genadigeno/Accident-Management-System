package ams.firerescue.building;

import java.util.List;

/** Fire-relevant building information for an address: escape routes and gas-line locations. */
public record BuildingPlan(
        String address,
        int floors,
        List<String> fireEscapeRoutes,
        List<String> gasLineLocations,
        String source) {
}
