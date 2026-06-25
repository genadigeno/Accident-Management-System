package ams.ui.app.service;

import ams.ui.app.config.DiscoveryProperties;
import ams.ui.app.dta.ServiceHealthView;
import ams.ui.app.model.ServiceDto;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Minimal service registry. Services to monitor are seeded from configuration and can also be
 * added at runtime via {@link #registerService}. The active poller ({@code ServiceCheck}) writes
 * the latest health snapshot for each one back here via {@link #updateHealth}.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RegistratorService {

    private final DiscoveryProperties properties;

    /** service name -> base URL to poll */
    private final Map<String, String> targets = new ConcurrentHashMap<>();
    /** service name -> latest health snapshot */
    private final Map<String, ServiceHealthView> health = new ConcurrentHashMap<>();

    @PostConstruct
    void seedFromConfig() {
        for (DiscoveryProperties.Target t : properties.getServices()) {
            targets.put(t.getName(), t.getUrl());
            health.put(t.getName(),
                    new ServiceHealthView(t.getName(), t.getUrl(), "UNKNOWN", 0, null, 0, "not yet checked"));
        }
        log.info("Service discovery seeded with {} service(s): {}", targets.size(), targets.keySet());
    }

    /** Adds (or refreshes) a service to monitor at runtime. */
    public void registerService(ServiceDto serviceDto) {
        if (serviceDto.getServiceName() == null || serviceDto.getServiceUrl() == null) {
            return;
        }
        String name = serviceDto.getServiceName();
        targets.put(name, serviceDto.getServiceUrl());
        health.putIfAbsent(name,
                new ServiceHealthView(name, serviceDto.getServiceUrl(), "UNKNOWN", 0, null, 0, "registered"));
        log.info("Registered service {} at {}", name, serviceDto.getServiceUrl());
    }

    /** Passive supplement: a heartbeat marks a service alive between active polls. */
    public void receiveHeartBeat(String serviceName) {
        ServiceHealthView cur = health.get(serviceName);
        if (cur != null) {
            health.put(serviceName, new ServiceHealthView(cur.name(), cur.url(), "UP",
                    cur.latencyMs(), cur.httpStatus(), System.currentTimeMillis(), "heartbeat"));
        }
    }

    public Map<String, String> targets() {
        return targets;
    }

    public void updateHealth(ServiceHealthView view) {
        health.put(view.name(), view);
    }

    public List<ServiceHealthView> snapshot() {
        return health.values().stream()
                .sorted(Comparator.comparing(ServiceHealthView::name))
                .toList();
    }
}
