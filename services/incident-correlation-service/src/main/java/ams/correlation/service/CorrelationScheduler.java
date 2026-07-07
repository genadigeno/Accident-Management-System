package ams.correlation.service;

import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Drives incident auto-closing. Lives outside {@link CorrelationService} so the call goes
 * through the transactional proxy (self-invocation would bypass it).
 */
@Component
@RequiredArgsConstructor
public class CorrelationScheduler {

    private final CorrelationService correlationService;

    @Scheduled(fixedDelayString = "${correlation.close-check-ms:30000}")
    public void closeIdle() {
        correlationService.closeIdleIncidents();
    }
}
