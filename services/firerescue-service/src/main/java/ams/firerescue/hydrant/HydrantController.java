package ams.firerescue.hydrant;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Nearby fire hydrants for an incident location, nearest first. */
@RestController
@RequestMapping("/api/v1/hydrants")
@RequiredArgsConstructor
public class HydrantController {

    private final HydrantService hydrantService;

    /** Example: {@code GET /api/v1/hydrants/nearby?lat=41.7&lng=44.8&radius=500}. */
    @GetMapping("/nearby")
    public List<Hydrant> nearby(@RequestParam double lat,
                                @RequestParam double lng,
                                @RequestParam(defaultValue = "500") int radius) {
        return hydrantService.findNearby(lat, lng, radius);
    }
}
