package ams.statistics.service;

import ams.statistics.jpa.StatisticalModelRepository;
import ams.statistics.jpa.StatisticalModelRepository.HourTotalRow;
import ams.statistics.jpa.StatisticalModelRepository.RollupRow;
import ams.statistics.jpa.StatisticalModelRepository.TypeTotal;
import ams.statistics.jpa.StatisticalModelRepository.TypeTotalRow;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Read-only queries over the windowed aggregates produced by the statistics pipeline.
 * For the metrics REST API.
 */
@Service
@RequiredArgsConstructor
public class StatisticsQueryService {

    private final StatisticalModelRepository repository;

    @Transactional(readOnly = true)
    public List<TypeCount> totalsByType() {
        return repository.totalsByType().stream()
                .map(t -> new TypeCount(t.getType().name(), t.getTotal()))
                .toList();
    }

    @Transactional(readOnly = true)
    public Summary summary() {
        List<TypeCount> byType = totalsByType();   // already ordered most-frequent first
        long total = repository.totalEvents();
        String mostCommon = byType.isEmpty() ? null : byType.get(0).type();
        return new Summary(total, mostCommon, byType);
    }

    @Transactional(readOnly = true)
    public List<WindowStat> recentWindows(int limit) {
        int capped = Math.max(1, Math.min(limit, 1000));
        return repository.findRecentWindows(PageRequest.of(0, capped)).stream()
                .map(s -> new WindowStat(
                        s.getId().getType().name(),
                        s.getId().getStart().toString(),
                        s.getId().getEnd().toString(),
                        s.getCount()))
                .toList();
    }

    private static final Set<String> ROLLUP_UNITS = Set.of("hour", "day", "week");

    /**
     * Rollup: events per bucket per type ({@code unit} = hour/day/week) for the last
     * {@code span} units. "Accidents per hour/day/week" from the original spec.
     */
    @Transactional(readOnly = true)
    public List<RollupBucket> rollup(String unit, int span) {
        if (!ROLLUP_UNITS.contains(unit)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "unit must be one of " + ROLLUP_UNITS);
        }
        int capped = Math.max(1, Math.min(span, 1000));
        LocalDateTime from = LocalDateTime.now().minus(unitDuration(unit).multipliedBy(capped));
        return repository.rollup(unit, from).stream()
                .map(r -> new RollupBucket(r.getBucket().toLocalDateTime().toString(), r.getType(), r.getTotal()))
                .toList();
    }

    /**
     * Period-over-period trend — "crime rate increased by 20%". Compares the last full
     * {@code period} (hour/day/week) against the one before it, overall and per type.
     * {@code changePercent} is null when the previous period had no events (no baseline).
     */
    @Transactional(readOnly = true)
    public Trend trend(String period) {
        if (!ROLLUP_UNITS.contains(period)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "period must be one of " + ROLLUP_UNITS);
        }
        LocalDateTime now = LocalDateTime.now();
        Duration span = unitDuration(period);
        LocalDateTime currentFrom = now.minus(span);
        LocalDateTime previousFrom = now.minus(span.multipliedBy(2));

        long current = repository.totalBetween(currentFrom, now);
        long previous = repository.totalBetween(previousFrom, currentFrom);

        Map<String, long[]> byType = new LinkedHashMap<>();   // type -> [current, previous]
        for (TypeTotalRow row : repository.totalsByTypeBetween(currentFrom, now)) {
            byType.computeIfAbsent(row.getType(), k -> new long[2])[0] = row.getTotal();
        }
        for (TypeTotalRow row : repository.totalsByTypeBetween(previousFrom, currentFrom)) {
            byType.computeIfAbsent(row.getType(), k -> new long[2])[1] = row.getTotal();
        }
        List<TypeTrend> typeTrends = byType.entrySet().stream()
                .map(e -> new TypeTrend(e.getKey(), e.getValue()[0], e.getValue()[1],
                        changePercent(e.getValue()[0], e.getValue()[1])))
                .toList();

        return new Trend(period, current, previous, changePercent(current, previous), typeTrends);
    }

    /** Events per hour of day, busiest first — "peak accident hours: 5 PM – 7 PM". */
    @Transactional(readOnly = true)
    public List<PeakHour> peakHours() {
        return repository.totalsByHourOfDay().stream()
                .map(r -> new PeakHour(r.getHour(), r.getTotal()))
                .toList();
    }

    /** Daily per-type totals as CSV — the "reports for city officials" from the original spec. */
    @Transactional(readOnly = true)
    public String dailyReportCsv(int days) {
        StringBuilder csv = new StringBuilder("date,type,total\n");
        for (RollupBucket bucket : rollup("day", days)) {
            csv.append(bucket.bucket(), 0, 10).append(',')
                    .append(bucket.type()).append(',')
                    .append(bucket.total()).append('\n');
        }
        return csv.toString();
    }

    private static Duration unitDuration(String unit) {
        return switch (unit) {
            case "hour" -> Duration.ofHours(1);
            case "day" -> Duration.ofDays(1);
            default -> Duration.ofDays(7);
        };
    }

    private static Double changePercent(long current, long previous) {
        if (previous == 0) {
            return null;   // no baseline
        }
        return Math.round((current - previous) * 1000.0 / previous) / 10.0;
    }

    public record TypeCount(String type, long total) {}

    public record WindowStat(String type, String windowStart, String windowEnd, long count) {}

    public record Summary(long totalEvents, String mostCommonType, List<TypeCount> byType) {}

    public record RollupBucket(String bucket, String type, long total) {}

    public record TypeTrend(String type, long current, long previous, Double changePercent) {}

    public record Trend(String period, long current, long previous, Double changePercent,
                        List<TypeTrend> byType) {}

    public record PeakHour(int hour, long total) {}
}
