package ams.correlation.api;

import ams.data.model.AccidentType;
import ams.correlation.domain.Incident;
import ams.correlation.domain.IncidentReport;
import ams.correlation.domain.IncidentReportRepository;
import ams.correlation.domain.IncidentRepository;
import ams.correlation.service.CorrelationService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.List;

/** Read API over correlated incidents (also serves the gateway's duplicate hint). */
@RestController
@RequestMapping("/api/v1/incidents")
@RequiredArgsConstructor
public class IncidentController {

    private final IncidentRepository incidentRepository;
    private final IncidentReportRepository reportRepository;
    private final CorrelationService correlationService;

    /** The 50 most recently active incidents, newest first. */
    @GetMapping
    public List<IncidentView> recent() {
        return incidentRepository.findTop50ByOrderByLastReportedAtDesc().stream()
                .map(i -> IncidentView.from(i, null)).toList();
    }

    /** One incident with the ids of all reports merged into it. */
    @GetMapping("/{id}")
    public IncidentView byId(@PathVariable String id) {
        Incident incident = incidentRepository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "no such incident"));
        List<String> reports = reportRepository.findByIncidentIdOrderByReportedAtAsc(id).stream()
                .map(IncidentReport::getReportId).toList();
        return IncidentView.from(incident, reports);
    }

    /**
     * The open incident a new report at this location would merge into — the gateway uses it
     * to tell callers "this is already reported". Empty list when the spot is clear.
     */
    @GetMapping("/nearby")
    public List<IncidentView> nearby(@RequestParam double lat,
                                     @RequestParam double lng,
                                     @RequestParam AccidentType type) {
        return correlationService.findOpenIncidentNearby(type, lat, lng)
                .map(i -> List.of(IncidentView.from(i, null)))
                .orElse(List.of());
    }

    public record IncidentView(String id, String status, String accidentType, String address,
                               Double latitude, Double longitude, int reportCount,
                               Instant firstReportedAt, Instant lastReportedAt,
                               List<String> reportIds) {
        static IncidentView from(Incident i, List<String> reportIds) {
            return new IncidentView(i.getId(), i.getState().name(), i.getAccidentType().name(),
                    i.getAddress(), i.getLatitude(), i.getLongitude(), i.getReportCount(),
                    i.getFirstReportedAt(), i.getLastReportedAt(), reportIds);
        }
    }
}
