package ams.firerescue.building;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Provides crews with fire escape routes and gas-line locations for a building. */
@RestController
@RequestMapping("/api/v1/buildings")
@RequiredArgsConstructor
public class BuildingPlanController {
    private final BuildingPlanService buildingPlanService;

    /** Example: {@code GET /api/v1/buildings/plan?address=12 Oak Street}. */
    @GetMapping("/plan")
    public BuildingPlan plan(@RequestParam String address) {
        return buildingPlanService.forAddress(address);
    }
}
