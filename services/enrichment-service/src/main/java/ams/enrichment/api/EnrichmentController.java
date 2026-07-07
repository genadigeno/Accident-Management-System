package ams.enrichment.api;

import ams.data.model.EnrichedAccidentEvent;
import ams.enrichment.service.EnrichmentService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;

/** Read API over the most recently enriched events (in-memory; the service has no database). */
@RestController
@RequestMapping("/api/v1/enriched")
@RequiredArgsConstructor
public class EnrichmentController {

    private final EnrichmentService enrichmentService;

    @GetMapping("/recent")
    public List<EnrichedView> recent() {
        return enrichmentService.recent().stream().map(EnrichedView::from).toList();
    }

    public record EnrichedView(String cacheId, String type, String address, String district,
                               String weatherCondition, Double temperatureC, Double precipitationMm,
                               Instant enrichedAt) {
        static EnrichedView from(EnrichedAccidentEvent e) {
            Double temp = e.getTemperatureC() == -999.0 ? null : e.getTemperatureC();
            Double precip = e.getPrecipitationMm() == -1.0 ? null : e.getPrecipitationMm();
            return new EnrichedView(str(e.getCacheId()), str(e.getType()), str(e.getAddress()),
                    str(e.getDistrict()), str(e.getWeatherCondition()), temp, precip, e.getEnrichedAt());
        }
        private static String str(CharSequence cs) {
            return cs == null ? "" : cs.toString();
        }
    }
}
