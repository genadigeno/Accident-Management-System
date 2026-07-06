package ams.lawenforcement.bolo;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BoloDetectorTest {

    private final BoloDetector detector = new BoloDetector();

    @Test
    void critical_on_weapon_keyword() {
        assertEquals(BoloLevel.CRITICAL, detector.detect("Reports of a man with a GUN near the bank"));
        assertEquals(BoloLevel.CRITICAL, detector.detect("possible hostage situation"));
    }

    @Test
    void high_on_stolen_vehicle() {
        assertEquals(BoloLevel.HIGH, detector.detect("a stolen vehicle was spotted heading north"));
        assertEquals(BoloLevel.HIGH, detector.detect("armed robbery at the store"));
    }

    @Test
    void critical_takes_precedence_over_high() {
        assertEquals(BoloLevel.CRITICAL, detector.detect("armed robbery, suspect has a gun"));
    }

    @Test
    void none_when_no_keywords_or_blank() {
        assertEquals(BoloLevel.NONE, detector.detect("noise complaint on Main Street"));
        assertEquals(BoloLevel.NONE, detector.detect(""));
        assertEquals(BoloLevel.NONE, detector.detect(null));
    }
}
