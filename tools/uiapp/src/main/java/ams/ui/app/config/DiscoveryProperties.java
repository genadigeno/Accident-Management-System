package ams.ui.app.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Configuration for active service discovery: the list of services to health-poll and the
 * thresholds used to classify them as UP / DEGRADED / DOWN.
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "discovery")
public class DiscoveryProperties {

    /** How often to poll every service's health endpoint. */
    private long pollIntervalMs = 5000;
    /** Connect/read timeout for a single health probe. */
    private int timeoutMs = 2000;
    /** A reachable service slower than this (ms) is reported DEGRADED rather than UP. */
    private long degradedLatencyMs = 500;
    /** Services to monitor. */
    private List<Target> services = new ArrayList<>();

    @Getter
    @Setter
    public static class Target {
        private String name;
        private String url;
    }
}
