package ams.enrichment.district;

/**
 * Resolves a district / area label for a coordinate. The default {@link GridDistrictResolver} is
 * offline and deterministic; a reverse-geocoding implementation (e.g. Nominatim) can be swapped
 * in behind this interface without touching the enrichment path.
 */
public interface DistrictResolver {
    String resolve(double latitude, double longitude);
}
