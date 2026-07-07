package ams.firerescue.hydrant;

/** A fire hydrant near an incident, with its great-circle distance from the query point. */
public record Hydrant(String ref, double latitude, double longitude, long distanceMeters) {
}
