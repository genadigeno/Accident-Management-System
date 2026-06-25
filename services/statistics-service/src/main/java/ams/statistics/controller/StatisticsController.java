package ams.statistics.controller;

import ams.statistics.service.StatisticsQueryService;
import ams.statistics.service.StatisticsQueryService.Summary;
import ams.statistics.service.StatisticsQueryService.TypeCount;
import ams.statistics.service.StatisticsQueryService.WindowStat;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Real-time metrics over the accident statistics aggregated by the windowing pipeline.
 * */
@RestController
@RequestMapping("/api/v1/stats")
@RequiredArgsConstructor
public class StatisticsController {

    private final StatisticsQueryService statisticsQueryService;

    // Total event count per accident type (most frequent first)
    @GetMapping("/by-type")
    public List<TypeCount> byType() {
        return statisticsQueryService.totalsByType();
    }

    // Headline summary: total events, the most common type, and the per-type breakdown
    @GetMapping("/summary")
    public Summary summary() {
        return statisticsQueryService.summary();
    }

    // The most recent windowed aggregates (default 50, max 1000)
    @GetMapping("/recent")
    public List<WindowStat> recent(@RequestParam(defaultValue = "50") int limit) {
        return statisticsQueryService.recentWindows(limit);
    }
}
