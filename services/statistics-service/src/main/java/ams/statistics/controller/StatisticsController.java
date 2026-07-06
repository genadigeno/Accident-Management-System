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

    // Events per hour for the last N hours (default 24), per accident type
    @GetMapping("/hourly")
    public List<StatisticsQueryService.RollupBucket> hourly(@RequestParam(defaultValue = "24") int hours) {
        return statisticsQueryService.rollup("hour", hours);
    }

    // Events per day for the last N days (default 30), per accident type
    @GetMapping("/daily")
    public List<StatisticsQueryService.RollupBucket> daily(@RequestParam(defaultValue = "30") int days) {
        return statisticsQueryService.rollup("day", days);
    }

    // Events per week for the last N weeks (default 12), per accident type
    @GetMapping("/weekly")
    public List<StatisticsQueryService.RollupBucket> weekly(@RequestParam(defaultValue = "12") int weeks) {
        return statisticsQueryService.rollup("week", weeks);
    }

    // Period-over-period change ("crime rate increased by 20%"); period = hour|day|week
    @GetMapping("/trend")
    public StatisticsQueryService.Trend trend(@RequestParam(defaultValue = "day") String period) {
        return statisticsQueryService.trend(period);
    }

    // Events per hour of day, busiest first ("peak accident hours: 5 PM - 7 PM")
    @GetMapping("/peak-hours")
    public List<StatisticsQueryService.PeakHour> peakHours() {
        return statisticsQueryService.peakHours();
    }
}
