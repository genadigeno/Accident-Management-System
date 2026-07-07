package ams.gateway.api;

import ams.gateway.security.ApiKeyFilter;
import ams.gateway.service.ReportService;
import ams.gateway.service.ReportService.AcceptedReport;
import ams.gateway.service.ReportService.ReportDraft;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** The public intake endpoint: citizens (or partner systems) report incidents here. */
@RestController
@RequestMapping("/api/v1/reports")
@RequiredArgsConstructor
public class ReportController {

    private final ReportService reportService;

    /**
     * Accepts a report, publishes it into the pipeline, and — when an open incident already
     * exists at that location — tells the caller it is likely already reported (the report is
     * still taken: extra callers add information, exactly like a real 911 center).
     */
    @PostMapping
    public ResponseEntity<AcceptedReport> submit(@RequestBody ReportDraft draft,
                                                 HttpServletRequest request) {
        String apiKey = request.getHeader(ApiKeyFilter.HEADER);
        String reporter = apiKey != null ? apiKey : request.getRemoteAddr();
        AcceptedReport accepted = reportService.accept(draft, reporter);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(accepted);
    }
}
