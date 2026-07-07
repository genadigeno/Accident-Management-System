package ams.lawenforcement.crime;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Crime analytics: geographic hotspots and their EWMA next-day forecast. */
@RestController
@RequestMapping("/api/v1/crime")
@RequiredArgsConstructor
public class CrimeController {

    private final CrimeAnalyticsService service;

    /** Busiest ~1km cells over the last N days (default 30), busiest first. */
    @GetMapping("/hotspots")
    public List<CrimeAnalyticsService.Hotspot> hotspots(@RequestParam(defaultValue = "30") int days,
                                                        @RequestParam(defaultValue = "20") int limit) {
        return service.hotspots(days, limit);
    }

    /**
     * EWMA next-day crime forecast for the busiest cells ("predict next likely crime location").
     * {@code alpha} weights recent days (0.05–0.95, default 0.5).
     */
    @GetMapping("/forecast")
    public List<CrimeAnalyticsService.Forecast> forecast(@RequestParam(defaultValue = "30") int days,
                                                         @RequestParam(defaultValue = "10") int limit,
                                                         @RequestParam(defaultValue = "0.5") double alpha) {
        return service.forecast(days, limit, alpha);
    }
}
