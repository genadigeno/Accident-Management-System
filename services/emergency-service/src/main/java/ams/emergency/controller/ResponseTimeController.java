package ams.emergency.controller;

import ams.emergency.response.ResponseTime;
import ams.emergency.response.ResponseTimeService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;

/** Read API over response times ("EMS arrived in 8 minutes") and SLA breaches. */
@RestController
@RequestMapping("/api/v1/response-times")
@RequiredArgsConstructor
public class ResponseTimeController {

    private final ResponseTimeService responseTimeService;

    /** The 50 most recent responses, newest first. */
    @GetMapping
    public List<ResponseView> recent() {
        return responseTimeService.recent().stream().map(ResponseView::from).toList();
    }

    /** Headline numbers: totals, average response, SLA breaches — overall and per unit type. */
    @GetMapping("/summary")
    public ResponseTimeService.Summary summary() {
        return responseTimeService.summary();
    }

    public record ResponseView(String dispatchId, String incidentId, String unitType, String unitId,
                               Long responseSeconds, boolean slaBreached,
                               Instant dispatchedAt, Instant onSceneAt, Instant clearedAt) {
        static ResponseView from(ResponseTime r) {
            return new ResponseView(r.getDispatchId(), r.getIncidentId(), r.getUnitType().name(),
                    r.getUnitId(), r.getResponseSeconds(), r.isSlaBreached(),
                    r.getDispatchedAt(), r.getOnSceneAt(), r.getClearedAt());
        }
    }
}
