package ams.ui.app.dta;

import java.util.List;
import java.util.Map;

/** Aggregated, real-time view of the accident stream, pushed to the dashboard every second. */
public record AnalyticsSnapshot(
        long totalEvents,
        double eventsPerSec,
        Map<String, Long> byType,
        List<LocationCount> topLocations,
        long sensitiveCount,
        long fraudCount,
        long timestamp) {

    public record LocationCount(String address, long count) {
    }
}
