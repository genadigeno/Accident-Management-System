package ams.firerescue.building;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StubBuildingPlanProviderTest {

    private final StubBuildingPlanProvider provider = new StubBuildingPlanProvider();

    @Test
    void deterministic_for_the_same_address() {
        BuildingPlan a = provider.findByAddress("12 Oak Street").orElseThrow();
        BuildingPlan b = provider.findByAddress("12 Oak Street").orElseThrow();
        assertEquals(a, b);
    }

    @Test
    void always_has_escape_routes_and_gas_lines() {
        BuildingPlan plan = provider.findByAddress("42 Maple Avenue, Townsville").orElseThrow();
        assertTrue(plan.floors() >= 1);
        assertFalse(plan.fireEscapeRoutes().isEmpty());
        assertFalse(plan.gasLineLocations().isEmpty());
    }

    @Test
    void empty_for_blank_address() {
        assertTrue(provider.findByAddress("").isEmpty());
        assertTrue(provider.findByAddress(null).isEmpty());
    }
}
