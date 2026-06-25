package ams.event.stream.geofence;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Locale;

/**
 * Geo-fencing: flags incidents whose address falls inside a configured "sensitive" zone
 * (government buildings, embassies, airports, ...). Matching is a simple case-insensitive
 * keyword check against the address; the keyword list is configurable via
 * {@code geofence.sensitive-keywords}.
 */
@Component
public class GeoFenceClassifier {

    private final List<String> sensitiveKeywords;

    public GeoFenceClassifier(
            @Value("${geofence.sensitive-keywords}") List<String> sensitiveKeywords) {
        this.sensitiveKeywords = sensitiveKeywords.stream()
                .map(keyword -> keyword.toLowerCase(Locale.ROOT).trim())
                .filter(keyword -> !keyword.isBlank())
                .toList();
    }

    public boolean isSensitive(String address) {
        if (address == null || address.isBlank()) {
            return false;
        }
        String lower = address.toLowerCase(Locale.ROOT);
        return sensitiveKeywords.stream().anyMatch(lower::contains);
    }
}
