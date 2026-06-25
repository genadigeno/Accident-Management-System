package ams.ui.app.dta;

/** Current health of one monitored service, sent to the dashboard over REST/WebSocket. */
public record ServiceHealthView(
        String name,
        String url,
        String status,      // UP / DEGRADED / DOWN / UNKNOWN
        long latencyMs,
        Integer httpStatus,
        long lastChecked,
        String detail) {
}
