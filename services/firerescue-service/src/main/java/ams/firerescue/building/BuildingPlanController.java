package ams.firerescue.building;

import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

/** Provides crews with fire escape routes and gas-line locations for a building. */
@RestController
@RequestMapping("/api/v1/buildings")
@RequiredArgsConstructor
public class BuildingPlanController {
    private final BuildingPlanService buildingPlanService;
    private final DbBuildingPlanProvider dbProvider;

    /** Example: {@code GET /api/v1/buildings/plan?address=12 Oak Street}. */
    @GetMapping("/plan")
    public BuildingPlan plan(@RequestParam String address) {
        return buildingPlanService.forAddress(address);
    }

    /** Import (or replace) a real building plan for an address; thereafter it is served from the DB. */
    @PostMapping("/plan")
    public ResponseEntity<BuildingPlan> importPlan(@RequestBody PlanRequest request) {
        if (request == null || request.address() == null || request.address().isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "address is required");
        }
        if (request.floors() < 1 || request.floors() > 300) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "floors must be between 1 and 300");
        }
        BuildingPlan saved = dbProvider.save(request.address(), request.floors(),
                request.fireEscapeRoutes(), request.gasLineLocations());
        return ResponseEntity.status(HttpStatus.CREATED).body(saved);
    }

    public record PlanRequest(String address, int floors,
                              List<String> fireEscapeRoutes, List<String> gasLineLocations) {}
}
