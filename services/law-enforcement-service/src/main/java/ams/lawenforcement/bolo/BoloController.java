package ams.lawenforcement.bolo;

import ams.lawenforcement.repository.LawEnforcementAccident;
import ams.lawenforcement.service.LawEnforcementService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Read API for BOLO ("Be On the Lookout") incidents. */
@RestController
@RequestMapping("/api/v1/bolo")
@RequiredArgsConstructor
public class BoloController {
    private final LawEnforcementService lawEnforcementService;

    // The 50 most recent active BOLO incidents (HIGH + CRITICAL), newest first
    @GetMapping
    public List<BoloView> active() {
        return lawEnforcementService.findTop50ByBoloLevelInOrderByIdDesc(BoloLevel.HIGH, BoloLevel.CRITICAL)
                .stream()
                .map(BoloView::from)
                .toList();
    }

    public record BoloView(Long id, String level, String address, String description, String date) {
        static BoloView from(LawEnforcementAccident a) {
            return new BoloView(
                    a.getId(),
                    a.getBoloLevel().name(),
                    a.getAddress(),
                    a.getDescription(),
                    a.getDate() == null ? null : a.getDate().toString());
        }
    }
}
