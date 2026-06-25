package ams.emergency.hospital;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Finds hospitals near an accident location so responders can be directed to the nearest one. */
@RestController
@RequestMapping("/api/v1/hospitals")
@RequiredArgsConstructor
public class HospitalController {

    private final HospitalService hospitalService;

    /**
     * Hospitals within {@code radius} metres of the given coordinates, nearest first.
     * Example: {@code GET /api/v1/hospitals/nearby?lat=41.7151&lng=44.8271&radius=5000}
     */
    @GetMapping("/nearby")
    public List<Hospital> nearby(
            @RequestParam double lat,
            @RequestParam double lng,
            @RequestParam(defaultValue = "5000") int radius) {
        return hospitalService.findNearby(lat, lng, radius);
    }
}
