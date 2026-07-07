package ams.search.service;

import ams.data.model.AccidentEventModel;
import ams.search.es.ElasticsearchGateway;
import ams.search.es.IncidentDocument;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/** Maps a reported incident to its searchable document and indexes it. */
@Slf4j
@Service
@RequiredArgsConstructor
public class IndexingService {

    private final ElasticsearchGateway es;
    private final MeterRegistry meterRegistry;

    public void index(AccidentEventModel event) {
        String cacheId = event.getCacheId() != null ? event.getCacheId().toString() : null;
        if (cacheId == null || cacheId.isBlank()) {
            throw new IllegalArgumentException("event has no cacheId — cannot index");
        }
        double lat = parse(event.getLocation() != null ? event.getLocation().getLatitude() : null);
        double lon = parse(event.getLocation() != null ? event.getLocation().getLongitude() : null);
        String address = event.getLocation() != null && event.getLocation().getAddress() != null
                ? event.getLocation().getAddress().toString() : "";

        IncidentDocument doc = new IncidentDocument(
                cacheId,
                event.getType() != null ? event.getType().name() : "UNKNOWN",
                event.getDescription() != null ? event.getDescription().toString() : "",
                address,
                new IncidentDocument.GeoPoint(lat, lon),
                System.currentTimeMillis());

        es.index(doc);
        meterRegistry.counter("ams.search.indexed").increment();
    }

    private static double parse(CharSequence value) {
        try {
            return value == null || value.isEmpty() ? 0.0 : Double.parseDouble(value.toString());
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }
}
