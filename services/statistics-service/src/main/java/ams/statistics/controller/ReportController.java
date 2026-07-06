package ams.statistics.controller;

import ams.statistics.service.StatisticsQueryService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;

/** Downloadable reports for city officials (original spec: "Generate PDF/CSV reports"). */
@RestController
@RequestMapping("/api/v1/reports")
@RequiredArgsConstructor
public class ReportController {

    private final StatisticsQueryService service;

    /** Daily per-type incident totals for the last N days as a CSV download. */
    @GetMapping(value = "/daily.csv", produces = "text/csv")
    public ResponseEntity<String> dailyCsv(@RequestParam(defaultValue = "30") int days) {
        String filename = "ams-daily-report-" + LocalDate.now() + ".csv";
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"")
                .contentType(MediaType.parseMediaType("text/csv"))
                .body(service.dailyReportCsv(days));
    }
}
