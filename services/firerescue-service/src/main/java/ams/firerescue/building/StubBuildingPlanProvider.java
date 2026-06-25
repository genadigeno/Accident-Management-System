package ams.firerescue.building;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Random;

/**
 * In-memory stand-in for a Fire Department blueprint database. Produces a deterministic,
 * plausible building plan for any address (same address → same plan) so the feature is
 * fully functional without an external system. Replace with a real provider in production.
 */
@Component
public class StubBuildingPlanProvider implements BuildingPlanProvider {

    private static final String[] STAIRWELLS =
            {"A (north side)", "B (south side)", "C (east side)", "D (west side)"};
    private static final String[] GAS_POINTS =
            {"basement NW corner", "ground-floor meter room", "utility closet", "roof plant room"};

    @Override
    public Optional<BuildingPlan> findByAddress(String address) {
        if (address == null || address.isBlank()) {
            return Optional.empty();
        }
        Random rnd = new Random(address.trim().toLowerCase(Locale.ROOT).hashCode());
        int floors = 1 + rnd.nextInt(20);

        List<String> routes = new ArrayList<>();
        int routeCount = 2 + rnd.nextInt(3); // 2–4 stairwell routes
        for (int i = 0; i < routeCount; i++) {
            routes.add("Stairwell " + STAIRWELLS[rnd.nextInt(STAIRWELLS.length)] + " — floors 1–" + floors);
        }
        routes.add("External fire escape — reachable from floor " + (1 + rnd.nextInt(floors)));

        List<String> gasLines = new ArrayList<>();
        gasLines.add("Main gas shutoff — " + GAS_POINTS[rnd.nextInt(GAS_POINTS.length)]);
        int extraGas = rnd.nextInt(3); // 0–2 additional risers
        for (int i = 0; i < extraGas; i++) {
            gasLines.add("Gas riser — floor " + (1 + rnd.nextInt(floors)) + ", " + GAS_POINTS[rnd.nextInt(GAS_POINTS.length)]);
        }

        return Optional.of(new BuildingPlan(address, floors, List.copyOf(routes), List.copyOf(gasLines), "stub"));
    }
}
