package ams.event.stream.geofence;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GeoFenceClassifierTest {

    private final GeoFenceClassifier classifier =
            new GeoFenceClassifier(List.of("government", "embassy", "airport"));

    @Test
    void sensitive_when_keyword_present() {
        assertTrue(classifier.isSensitive("12 Government Plaza, Cityville"));
        assertTrue(classifier.isSensitive("incident near the US EMBASSY"));
        assertTrue(classifier.isSensitive("Terminal 2, Airport Road"));
    }

    @Test
    void not_sensitive_otherwise() {
        assertFalse(classifier.isSensitive("42 Maple Street, Townsville"));
        assertFalse(classifier.isSensitive(""));
        assertFalse(classifier.isSensitive(null));
    }
}
