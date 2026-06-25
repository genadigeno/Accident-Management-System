package ams.lawenforcement.bolo;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Locale;

/**
 * Automatic BOLO ("Be On the Lookout") detection. Scans an incident description for threat
 * keywords and classifies its severity, raising a log alert and a Micrometer counter
 * ({@code ams.bolo.alerts}, tagged by level) for anything above {@link BoloLevel#NONE}.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class BoloDetector {

    private static final List<String> CRITICAL_KEYWORDS =
            List.of("gun", "firearm", "weapon", "hostage", "bomb", "explosive", "shooting", "shooter");
    private static final List<String> HIGH_KEYWORDS =
            List.of("stolen vehicle", "stolen car", "armed robbery", "robbery", "kidnap");

    private final MeterRegistry meterRegistry;

    public BoloLevel detect(String description) {
        if (description == null || description.isBlank()) {
            return BoloLevel.NONE;
        }

        String text = description.toLowerCase(Locale.ROOT);
        BoloLevel level = BoloLevel.NONE;

        if (containsAny(text, CRITICAL_KEYWORDS)) {
            level = BoloLevel.CRITICAL;
        }
        else if (containsAny(text, HIGH_KEYWORDS)) {
            level = BoloLevel.HIGH;
        }

        if (level != BoloLevel.NONE) {
            Counter.builder("ams.bolo.alerts")
                    .tag("level", level.name())
                    .description("Number of BOLO alerts raised, by severity")
                    .register(meterRegistry)
                    .increment();

            log.warn("BOLO [{}] raised for description: \"{}\"", level, description);
        }
        return level;
    }

    private static boolean containsAny(String text, List<String> keywords) {
        return keywords.stream().anyMatch(text::contains);
    }
}
