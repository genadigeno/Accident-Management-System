package ams.emergency.hospital;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Nearest-hospital lookup backed by the free OpenStreetMap Overpass API (no API key required).
 * Results are mapped to {@link Hospital} and sorted by great-circle (Haversine) distance.
 */
@Slf4j
@Component
public class OverpassHospitalProvider implements HospitalProvider {

    private static final double EARTH_RADIUS_M = 6_371_000;

    private final RestClient restClient;
    private final String overpassUrl;

    public OverpassHospitalProvider(
            RestClient.Builder builder,
            @Value("${hospital.overpass.url}") String overpassUrl,
            @Value("${hospital.overpass.connect-timeout-ms:3000}") int connectTimeoutMs,
            // 5s default: this sits on a dispatcher request path — a slow Overpass must fail
            // fast (the caller degrades to 502) rather than pin a server thread for 30s.
            @Value("${hospital.overpass.read-timeout-ms:5000}") int readTimeoutMs) {

        this.overpassUrl = overpassUrl;
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(connectTimeoutMs);
        factory.setReadTimeout(readTimeoutMs);
        this.restClient = builder.requestFactory(factory).build();
    }

    @Override
    public List<Hospital> findNearby(double latitude, double longitude, int radiusMeters) {
        String query = """
                [out:json][timeout:25];
                (
                  node["amenity"="hospital"](around:%d,%f,%f);
                  way["amenity"="hospital"](around:%d,%f,%f);
                  relation["amenity"="hospital"](around:%d,%f,%f);
                );
                out center;
                """.formatted(
                radiusMeters, latitude, longitude,
                radiusMeters, latitude, longitude,
                radiusMeters, latitude, longitude);

        OverpassResponse response = restClient.post()
                .uri(overpassUrl)
                .body(query)
                .retrieve()
                .body(OverpassResponse.class);

        if (response == null || response.elements() == null) {
            return List.of();
        }
        return response.elements().stream()
                .map(element -> toHospital(element, latitude, longitude))
                .filter(Objects::nonNull)
                .sorted(Comparator.comparingLong(Hospital::distanceMeters))
                .toList();
    }

    private static Hospital toHospital(Element element, double fromLat, double fromLng) {
        Double lat = element.lat() != null ? element.lat()
                : (element.center() != null ? element.center().lat() : null);
        Double lon = element.lon() != null ? element.lon()
                : (element.center() != null ? element.center().lon() : null);
        if (lat == null || lon == null) {
            return null;
        }
        String name = (element.tags() != null && element.tags().get("name") != null)
                ? element.tags().get("name") : "Unnamed hospital";
        long distance = Math.round(haversineMeters(fromLat, fromLng, lat, lon));
        return new Hospital(name, lat, lon, distance);
    }

    private static double haversineMeters(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        return EARTH_RADIUS_M * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record OverpassResponse(List<Element> elements) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    record Element(Double lat, Double lon, Center center, Map<String, String> tags) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    record Center(double lat, double lon) {}
}
