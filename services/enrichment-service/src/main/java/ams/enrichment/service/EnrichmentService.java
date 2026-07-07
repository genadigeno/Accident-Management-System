package ams.enrichment.service;

import ams.data.model.AccidentEventModel;
import ams.data.model.EnrichedAccidentEvent;
import ams.enrichment.district.DistrictResolver;
import ams.enrichment.weather.Weather;
import ams.enrichment.weather.WeatherProvider;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Enriches a reported incident with weather (at its location) and a district label, and publishes
 * the result to {@code accident.events.enriched}. Enrichment never fails the event: an
 * unreachable weather API just yields {@code weatherCondition = "unknown"}.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class EnrichmentService {

    private final WeatherProvider weatherProvider;
    private final DistrictResolver districtResolver;
    private final KafkaTemplate<String, EnrichedAccidentEvent> enrichedKafkaTemplate;
    private final MeterRegistry meterRegistry;

    @Value("${kafka.enriched.topic}")
    private String enrichedTopic;

    /** Newest-first ring of recently enriched events, for the read API (the service has no DB). */
    private final ConcurrentLinkedDeque<EnrichedAccidentEvent> recent = new ConcurrentLinkedDeque<>();
    private static final int RECENT_MAX = 50;

    public void enrichAndPublish(AccidentEventModel event) {
        String cacheId = event.getCacheId() != null ? event.getCacheId().toString() : null;
        if (cacheId == null || cacheId.isBlank()) {
            throw new IllegalArgumentException("event has no cacheId — cannot enrich");
        }
        String address = str(event.getLocation() != null ? event.getLocation().getAddress() : null);
        String latStr = str(event.getLocation() != null ? event.getLocation().getLatitude() : null);
        String lonStr = str(event.getLocation() != null ? event.getLocation().getLongitude() : null);
        double lat = parse(latStr);
        double lon = parse(lonStr);

        String district = districtResolver.resolve(lat, lon);
        Weather weather = weatherProvider.at(lat, lon);
        if (weather.temperatureC() == -999.0) {
            meterRegistry.counter("ams.enrichment.weather_unavailable").increment();
        }

        EnrichedAccidentEvent enriched = EnrichedAccidentEvent.newBuilder()
                .setCacheId(cacheId)
                .setType(event.getType() != null ? event.getType().name() : "UNKNOWN")
                .setDate(event.getDate() != null ? event.getDate().toString() : "")
                .setAddress(address)
                .setLatitude(latStr)
                .setLongitude(lonStr)
                .setDescription(str(event.getDescription()))
                .setDistrict(district)
                .setWeatherCondition(weather.condition())
                .setTemperatureC(weather.temperatureC())
                .setPrecipitationMm(weather.precipitationMm())
                .setEnrichedAt(Instant.now())
                .build();

        enrichedKafkaTemplate.send(enrichedTopic, cacheId, enriched);
        meterRegistry.counter("ams.enrichment.enriched").increment();

        recent.addFirst(enriched);
        while (recent.size() > RECENT_MAX) {
            recent.pollLast();
        }
    }

    /** The most recently enriched events (newest first) for the read API. */
    public List<EnrichedAccidentEvent> recent() {
        return List.copyOf(recent);
    }

    private static String str(CharSequence cs) {
        return cs == null ? "" : cs.toString();
    }

    private static double parse(String value) {
        try {
            return value == null || value.isEmpty() ? 0.0 : Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }
}
