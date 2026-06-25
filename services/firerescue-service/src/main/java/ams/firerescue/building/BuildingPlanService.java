package ams.firerescue.building;

import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

/**
 * Looks up the fire-relevant building plan for an address via the configured provider.
 * */
@Service
@RequiredArgsConstructor
public class BuildingPlanService {
    private final BuildingPlanProvider buildingPlanProvider;

    public BuildingPlan forAddress(String address) {
        if (address == null || address.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "address is required");
        }
        return buildingPlanProvider.findByAddress(address)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "no building plan for that address"));
    }
}
