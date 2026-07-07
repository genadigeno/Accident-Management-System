package ams.search.api;

import ams.search.service.SearchService;
import ams.search.service.SearchService.SearchResult;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Search over the indexed incident history: full-text, and geo/time "past incidents nearby". */
@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class SearchController {

    private final SearchService searchService;

    /**
     * Full-text search over incident descriptions, optionally filtered by type and recency.
     * Example: {@code /api/v1/search?q=robbery&type=CRIMINAL&days=60}.
     */
    @GetMapping("/search")
    public SearchResult search(@RequestParam(required = false) String q,
                               @RequestParam(required = false) String type,
                               @RequestParam(required = false) Integer days,
                               @RequestParam(defaultValue = "20") int size) {
        return searchService.search(q, type, days, size);
    }

    /**
     * Past incidents within a radius of a point — "this is the 3rd robbery at this bank in
     * 2 months". {@code total} is the answer; {@code hits} are the incidents.
     * Example: {@code /api/v1/history/nearby?lat=41.7&lng=44.8&radiusMeters=200&type=CRIMINAL&days=60}.
     */
    @GetMapping("/history/nearby")
    public SearchResult historyNearby(@RequestParam double lat,
                                      @RequestParam double lng,
                                      @RequestParam(defaultValue = "500") int radiusMeters,
                                      @RequestParam(required = false) String type,
                                      @RequestParam(required = false) Integer days,
                                      @RequestParam(defaultValue = "20") int size) {
        return searchService.nearby(lat, lng, radiusMeters, type, days, size);
    }
}
