package ams.enrichment.district;

import org.springframework.stereotype.Component;

/**
 * Deterministic, offline district label from a coordinate grid (~5&nbsp;km cells). Stands in for
 * a real reverse-geocoder so enrichment has no external dependency or rate limit on the stream;
 * the same location always maps to the same district.
 */
@Component
public class GridDistrictResolver implements DistrictResolver {

    private static final double CELL_DEGREES = 0.05;

    @Override
    public String resolve(double latitude, double longitude) {
        if (latitude == 0 && longitude == 0) {
            return "unknown";
        }
        int latCell = (int) Math.floor(latitude / CELL_DEGREES);
        int lngCell = (int) Math.floor(longitude / CELL_DEGREES);
        return "District " + Math.floorMod(latCell, 100) + "-" + Math.floorMod(lngCell, 100);
    }
}
