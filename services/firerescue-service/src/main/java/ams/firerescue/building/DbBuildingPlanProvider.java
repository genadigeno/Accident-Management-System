package ams.firerescue.building;

import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * Primary building-plan provider: returns the real plan from the database when the address is
 * known, and falls back to the {@link StubBuildingPlanProvider} otherwise, so the endpoint always
 * has an answer while a real blueprint database is being populated. Import plans via
 * {@link #save} (the POST endpoint).
 */
@Primary
@Component
@RequiredArgsConstructor
public class DbBuildingPlanProvider implements BuildingPlanProvider {

    private final BuildingPlanRepository repository;
    private final StubBuildingPlanProvider stub;

    @Override
    @Transactional(readOnly = true)
    public Optional<BuildingPlan> findByAddress(String address) {
        if (address == null || address.isBlank()) {
            return Optional.empty();
        }
        return repository.findByAddressKey(key(address))
                .map(DbBuildingPlanProvider::toPlan)
                .or(() -> stub.findByAddress(address));
    }

    /** Upsert a real plan (import endpoint). */
    @Transactional
    public BuildingPlan save(String address, int floors, List<String> routes, List<String> gasLines) {
        String key = key(address);
        BuildingPlanEntity entity = repository.findByAddressKey(key).orElseGet(BuildingPlanEntity::new);
        entity.setAddressKey(key);
        entity.setAddress(address.trim());
        entity.setFloors(floors);
        entity.setFireEscapeRoutes(join(routes));
        entity.setGasLineLocations(join(gasLines));
        return toPlan(repository.save(entity));
    }

    private static BuildingPlan toPlan(BuildingPlanEntity e) {
        return new BuildingPlan(e.getAddress(), e.getFloors(),
                split(e.getFireEscapeRoutes()), split(e.getGasLineLocations()), "db");
    }

    private static String key(String address) {
        return address.trim().toLowerCase(Locale.ROOT);
    }

    private static String join(List<String> values) {
        return values == null ? "" : String.join("\n", values);
    }

    private static List<String> split(String value) {
        return value == null || value.isBlank() ? List.of() : List.of(value.split("\n"));
    }
}
