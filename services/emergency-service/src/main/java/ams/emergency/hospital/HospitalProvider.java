package ams.emergency.hospital;

import java.util.List;

/**
 * Adapter for a nearest-hospital lookup backend. Implementations wrap a specific provider
 * (OpenStreetMap Overpass, Google Places, HERE, ...) so the service and controller stay
 * provider-agnostic.
 */
public interface HospitalProvider {

    /** Hospitals within {@code radiusMeters} of the point, nearest first. */
    List<Hospital> findNearby(double latitude, double longitude, int radiusMeters);
}
