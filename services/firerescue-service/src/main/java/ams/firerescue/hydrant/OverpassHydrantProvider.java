package ams.firerescue.hydrant;

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
 * Nearby fire-hydrant lookup backed by the free OpenStreetMap Overpass API (no API key,
 * {@code emergency=fire_hydrant} nodes). Results are sorted by great-circle (Haversine) distance.
 * Mirrors the emergency service's hospital provider.
 */
@Slf4j
@Component
public class OverpassHydrantProvider implements HydrantProvider {

    private static final double EARTH_RADIUS_M = 6_371_000;

    private final RestClient restClient;
    private final String overpassUrl;

    public OverpassHydrantProvider(
            RestClient.Builder builder,
            @Value("${hydrant.overpass.url:https://overpass-api.de/api/interpreter}") String overpassUrl,
            @Value("${hydrant.overpass.connect-timeout-ms:3000}") int connectTimeoutMs,
            // 5s: this sits on a request path — a slow Overpass must fail fast (caller degrades
            // to 502) rather than pin a server thread.
            @Value("${hydrant.overpass.read-timeout-ms:5000}") int readTimeoutMs) {
        this.overpassUrl = overpassUrl;
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(connectTimeoutMs);
        factory.setReadTimeout(readTimeoutMs);
        this.restClient = builder.requestFactory(factory).build();
    }

    @Override
    public List<Hydrant> findNearby(double latitude, double longitude, int radiusMeters) {
        String query = """
                [out:json][timeout:25];
                node["emergency"="fire_hydrant"](around:%d,%f,%f);
                out;
                """.formatted(radiusMeters, latitude, longitude);

        OverpassResponse response = restClient.post()
                .uri(overpassUrl)
                .body(query)
                .retrieve()
                .body(OverpassResponse.class);

        if (response == null || response.elements() == null) {
            return List.of();
        }
        return response.elements().stream()
                .map(element -> toHydrant(element, latitude, longitude))
                .filter(Objects::nonNull)
                .sorted(Comparator.comparingLong(Hydrant::distanceMeters))
                .toList();
    }

    private static Hydrant toHydrant(Element element, double fromLat, double fromLng) {
        if (element.lat() == null || element.lon() == null) {
            return null;
        }
        String ref = element.tags() != null && element.tags().get("ref") != null
                ? element.tags().get("ref") : "hydrant";
        long distance = Math.round(haversineMeters(fromLat, fromLng, element.lat(), element.lon()));
        return new Hydrant(ref, element.lat(), element.lon(), distance);
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
    record Element(Double lat, Double lon, Map<String, String> tags) {}
}
