package ams.dispatch.service;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Drives the dispatch lifecycle. Lives outside {@link DispatchService} so the calls go through
 * the transactional proxy (self-invocation would bypass it).
 *
 * <p>The simulator stands in for real units reporting status over radio/MDT; disable it
 * ({@code dispatch.simulator.enabled=false}) once real units feed {@code unit.status.events}.
 */
@Component
@RequiredArgsConstructor
public class DispatchScheduler {

    private final DispatchService dispatchService;

    @Value("${dispatch.simulator.enabled:true}")
    private boolean simulatorEnabled;

    /** Call stacking: assign queued incidents as units free up. */
    @Scheduled(fixedDelayString = "${dispatch.stacking.retry-ms:3000}")
    public void retryWaiting() {
        dispatchService.assignWaiting();
    }

    /** Simulator tick: advance active dispatches through their lifecycle. */
    @Scheduled(fixedDelayString = "${dispatch.simulator.tick-ms:2000}")
    public void advance() {
        if (simulatorEnabled) {
            dispatchService.advanceDue();
        }
    }
}
