package ams.ui.app.controller;

import com.sun.management.OperatingSystemMXBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.lang.management.ManagementFactory;

/**
 * Reports the capabilities of the host running the dashboard (CPU / RAM / JVM) and derives
 * rough capacity recommendations: how many service instances fit and how many events/sec the
 * pipeline can sustain. Heuristics, not guarantees — clearly labelled as estimates in the UI.
 */
@RestController
@RequestMapping("/api/v1/system")
public class SystemInfoController {

    /** Typical resident footprint of one AMS Spring Boot service. */
    private static final int PER_SERVICE_MB = 450;

    @GetMapping
    public SystemInfo system() {
        OperatingSystemMXBean os = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        int cpuThreads = Runtime.getRuntime().availableProcessors();
        double totalGb = os.getTotalMemorySize() / 1_073_741_824.0;
        double freeGb = os.getFreeMemorySize() / 1_073_741_824.0;
        long jvmMaxHeapMb = Runtime.getRuntime().maxMemory() / (1024 * 1024);
        double cpuLoad = os.getCpuLoad();   // 0..1, or negative if unavailable

        // Reserve ~half the RAM for the Kafka/Postgres/Elasticsearch infrastructure + the OS;
        // budget the rest for JVM services at ~PER_SERVICE_MB each.
        double usableForServicesGb = totalGb * 0.5;
        int maxInstancesByRam = (int) Math.floor(usableForServicesGb * 1024 / PER_SERVICE_MB);
        int recommendedConcurrentConsumers = cpuThreads;   // ~one busy consumer thread per logical core
        int estLow = cpuThreads * 120;                      // exactly-once fan-out, conservative
        int estHigh = cpuThreads * 250;

        Recommendations rec = new Recommendations(
                maxInstancesByRam,
                recommendedConcurrentConsumers,
                estLow + " to " + estHigh + " events/sec end-to-end (bursts higher)",
                "RAM caps how many instances fit; CPU (" + cpuThreads
                        + " threads) caps sustained throughput. ~half the RAM is reserved for Kafka/DB/ES.");

        return new SystemInfo(
                System.getProperty("os.name") + " " + System.getProperty("os.version"),
                System.getProperty("os.arch"),
                cpuThreads,
                round(totalGb),
                round(freeGb),
                jvmMaxHeapMb,
                cpuLoad < 0 ? null : Math.round(cpuLoad * 1000) / 10.0,
                rec);
    }

    private static double round(double v) {
        return Math.round(v * 10) / 10.0;
    }

    public record Recommendations(int maxServiceInstances, int recommendedConcurrentConsumers,
                                  String estimatedThroughput, String note) {}

    public record SystemInfo(String os, String arch, int cpuThreads, double totalRamGb, double freeRamGb,
                             long jvmMaxHeapMb, Double cpuLoadPct, Recommendations recommendations) {}
}
