package ams.lawenforcement.crime;

import ams.lawenforcement.repository.LawEnforcementRepository;
import ams.lawenforcement.repository.LawEnforcementRepository.DailyCellRow;
import ams.lawenforcement.repository.LawEnforcementRepository.HotspotRow;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Crime analytics over the persisted incidents: geographic <b>hotspots</b> (busiest ~1&nbsp;km
 * cells) and a lightweight <b>EWMA forecast</b> of each hotspot's next-day crime count — an
 * exponentially-weighted moving average over the daily series, the honest baseline the roadmap
 * calls for (a real model can replace it behind this service).
 */
@Service
@RequiredArgsConstructor
public class CrimeAnalyticsService {

    private final LawEnforcementRepository repository;

    @Transactional(readOnly = true)
    public List<Hotspot> hotspots(int days, int limit) {
        LocalDate from = LocalDate.now().minusDays(clampDays(days));
        return repository.hotspots(from, clampLimit(limit)).stream()
                .map(CrimeAnalyticsService::toHotspot)
                .toList();
    }

    /**
     * EWMA next-day forecast for the busiest cells. Each cell's daily counts are expanded to a
     * gap-filled series (missing days = 0) so a cell that has gone quiet decays toward zero, then
     * smoothed with {@code alpha} (higher = more weight on recent days).
     */
    @Transactional(readOnly = true)
    public List<Forecast> forecast(int days, int limit, double alpha) {
        int window = clampDays(days);
        double a = Math.min(Math.max(alpha, 0.05), 0.95);
        LocalDate from = LocalDate.now().minusDays(window);
        LocalDate today = LocalDate.now();

        // cellKey -> (day -> count)
        Map<String, Map<LocalDate, Long>> byCell = new LinkedHashMap<>();
        Map<String, double[]> latLng = new LinkedHashMap<>();
        Map<String, Long> totals = new LinkedHashMap<>();
        for (DailyCellRow row : repository.dailyCellCounts(from)) {
            String key = row.getCellLat() + "," + row.getCellLng();
            byCell.computeIfAbsent(key, k -> new LinkedHashMap<>()).put(row.getDay(), row.getTotal());
            latLng.putIfAbsent(key, new double[]{row.getCellLat().doubleValue(), row.getCellLng().doubleValue()});
            totals.merge(key, row.getTotal(), Long::sum);
        }

        return totals.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .limit(clampLimit(limit))
                .map(e -> {
                    String key = e.getKey();
                    Map<LocalDate, Long> series = byCell.get(key);
                    List<Long> daily = new ArrayList<>();
                    double ewma = 0.0;
                    boolean seeded = false;
                    for (LocalDate d = from; !d.isAfter(today); d = d.plusDays(1)) {
                        long v = series.getOrDefault(d, 0L);
                        daily.add(v);
                        ewma = seeded ? a * v + (1 - a) * ewma : v;
                        seeded = true;
                    }
                    double[] ll = latLng.get(key);
                    List<Long> recent = daily.subList(Math.max(0, daily.size() - 14), daily.size());
                    double predicted = Math.round(ewma * 100.0) / 100.0;
                    return new Forecast(ll[0], ll[1], e.getValue(), List.copyOf(recent), predicted);
                })
                .toList();
    }

    private static Hotspot toHotspot(HotspotRow r) {
        return new Hotspot(r.getCellLat().doubleValue(), r.getCellLng().doubleValue(),
                r.getTotal(), r.getBoloCount(), r.getSampleAddress());
    }

    private static int clampDays(int days) {
        return Math.min(Math.max(days, 1), 3650);
    }

    private static int clampLimit(int limit) {
        return Math.min(Math.max(limit, 1), 100);
    }

    public record Hotspot(double latitude, double longitude, long total, long boloCount, String sampleAddress) {}

    public record Forecast(double latitude, double longitude, long recentTotal,
                           List<Long> dailyCounts, double predictedNextDay) {}
}
