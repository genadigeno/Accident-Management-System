package ams.firerescue.hydrant;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.server.ResponseStatusException;

import java.time.Duration;
import java.util.List;
import java.util.Locale;

/**
 * Nearby fire-hydrant lookup. Validates the request, delegates to the {@link HydrantProvider},
 * and caches results by ~100m-rounded coordinate for a short TTL so repeated lookups around one
 * incident don't hammer the public Overpass API. Degrades to HTTP 502 when the provider is down.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class HydrantService {

    private final HydrantProvider provider;

    private final Cache<String, List<Hydrant>> cache = Caffeine.newBuilder()
            .maximumSize(500)
            .expireAfterWrite(Duration.ofMinutes(10))
            .build();

    public List<Hydrant> findNearby(double latitude, double longitude, int radiusMeters) {
        validate(latitude, longitude, radiusMeters);
        String key = String.format(Locale.ROOT, "%.3f:%.3f:%d", latitude, longitude, radiusMeters);
        try {
            return cache.get(key, k -> provider.findNearby(latitude, longitude, radiusMeters));
        } catch (RestClientException e) {
            log.warn("hydrant provider unavailable: {}", e.getMessage());
            throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, "hydrant lookup provider is unavailable");
        }
    }

    private static void validate(double latitude, double longitude, int radiusMeters) {
        if (latitude < -90 || latitude > 90) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "latitude must be between -90 and 90");
        }
        if (longitude < -180 || longitude > 180) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "longitude must be between -180 and 180");
        }
        if (radiusMeters < 50 || radiusMeters > 10_000) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "radius must be between 50 and 10000 metres");
        }
    }
}
