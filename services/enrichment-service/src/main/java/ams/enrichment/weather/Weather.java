package ams.enrichment.weather;

/** Weather at a location. {@code temperatureC == -999} / {@code precipitationMm == -1} = unavailable. */
public record Weather(String condition, double temperatureC, double precipitationMm) {

    public static Weather unavailable() {
        return new Weather("unknown", -999.0, -1.0);
    }
}
