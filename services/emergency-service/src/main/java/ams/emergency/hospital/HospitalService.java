package ams.emergency.hospital;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

/**
 * Nearest-hospital lookup. Validates the request and delegates to the configured
 * {@link HospitalProvider}, degrading gracefully (HTTP 502) when the external provider
 * is unreachable instead of leaking a stack trace.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class HospitalService {

    private final HospitalProvider provider;

    public List<Hospital> findNearby(double latitude, double longitude, int radiusMeters) {
        validate(latitude, longitude, radiusMeters);
        try {
            return provider.findNearby(latitude, longitude, radiusMeters);
        } catch (RestClientException e) {
            log.warn("hospital provider unavailable: {}", e.getMessage());
            throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, "hospital lookup provider is unavailable");
        }
    }

    private static void validate(double latitude, double longitude, int radiusMeters) {
        if (latitude < -90 || latitude > 90) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "latitude must be between -90 and 90");
        }
        if (longitude < -180 || longitude > 180) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "longitude must be between -180 and 180");
        }
        if (radiusMeters < 100 || radiusMeters > 50_000) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "radius must be between 100 and 50000 metres");
        }
    }
}
