package ams.emergency.hospital;

/** A hospital near an accident location, with its great-circle distance from that location. */
public record Hospital(String name, double latitude, double longitude, long distanceMeters) {
}
