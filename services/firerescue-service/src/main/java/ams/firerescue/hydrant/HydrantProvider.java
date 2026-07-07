package ams.firerescue.hydrant;

import java.util.List;

/**
 * Adapter for a hydrant backend. The default queries the free OpenStreetMap Overpass API; a
 * water-authority feed can be swapped in without touching the service or controller.
 */
public interface HydrantProvider {
    List<Hydrant> findNearby(double latitude, double longitude, int radiusMeters);
}
