package ams.lawenforcement.bolo;

import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Locale;

/**
 * Automatic BOLO ("Be On the Lookout") detection. Scans an incident description for threat
 * keywords and classifies its severity.
 *
 * <p>Pure classification — no side effects. The {@code ams.bolo.alerts} metric and the alert
 * log are raised in {@code LawEnforcementService} only for records that are actually persisted;
 * doing it here inflated the metric on every batch retry (observed 8 alerts for 2 incidents).
 */
@Component
public class BoloDetector {

    private static final List<String> CRITICAL_KEYWORDS =
            List.of("gun", "firearm", "weapon", "hostage", "bomb", "explosive", "shooting", "shooter");
    private static final List<String> HIGH_KEYWORDS =
            List.of("stolen vehicle", "stolen car", "armed robbery", "robbery", "kidnap");

    public BoloLevel detect(String description) {
        if (description == null || description.isBlank()) {
            return BoloLevel.NONE;
        }
        String text = description.toLowerCase(Locale.ROOT);

        if (containsAny(text, CRITICAL_KEYWORDS)) {
            return BoloLevel.CRITICAL;
        }
        if (containsAny(text, HIGH_KEYWORDS)) {
            return BoloLevel.HIGH;
        }
        return BoloLevel.NONE;
    }

    private static boolean containsAny(String text, List<String> keywords) {
        return keywords.stream().anyMatch(text::contains);
    }
}
