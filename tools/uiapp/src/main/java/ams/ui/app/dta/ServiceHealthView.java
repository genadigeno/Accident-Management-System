package ams.ui.app.dta;

/** Current health of one monitored service, sent to the dashboard over REST/WebSocket. */
public record ServiceHealthView(
        String name,
        String url,
        String status,      // UP / DEGRADED / DOWN / UNKNOWN
        long latencyMs,
        Integer httpStatus,
        long lastChecked,
        String detail,
        Long received,      // events consumed by the service (ams.events.received); null if not reported
        Long processed) {   // events processed successfully (ams.events.processed); null if not reported

    /** Convenience for the states that don't carry throughput counters. */
    public static ServiceHealthView of(String name, String url, String status, long latencyMs,
                                       Integer httpStatus, long lastChecked, String detail) {
        return new ServiceHealthView(name, url, status, latencyMs, httpStatus, lastChecked, detail, null, null);
    }
}
