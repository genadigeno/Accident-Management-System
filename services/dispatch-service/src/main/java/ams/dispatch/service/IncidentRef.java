package ams.dispatch.service;

/** The routing-relevant slice of a responder event. */
public record IncidentRef(String cacheId, String address, double latitude, double longitude) {
}
