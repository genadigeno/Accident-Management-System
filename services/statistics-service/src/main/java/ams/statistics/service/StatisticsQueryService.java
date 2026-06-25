package ams.statistics.service;

import ams.statistics.jpa.StatisticalModelRepository;
import ams.statistics.jpa.StatisticalModelRepository.TypeTotal;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Read-only queries over the windowed aggregates produced by the statistics pipeline.
 * For the metrics REST API.
 */
@Service
@RequiredArgsConstructor
public class StatisticsQueryService {

    private final StatisticalModelRepository repository;

    @Transactional(readOnly = true)
    public List<TypeCount> totalsByType() {
        return repository.totalsByType().stream()
                .map(t -> new TypeCount(t.getType().name(), t.getTotal()))
                .toList();
    }

    @Transactional(readOnly = true)
    public Summary summary() {
        List<TypeCount> byType = totalsByType();   // already ordered most-frequent first
        long total = repository.totalEvents();
        String mostCommon = byType.isEmpty() ? null : byType.get(0).type();
        return new Summary(total, mostCommon, byType);
    }

    @Transactional(readOnly = true)
    public List<WindowStat> recentWindows(int limit) {
        int capped = Math.max(1, Math.min(limit, 1000));
        return repository.findRecentWindows(PageRequest.of(0, capped)).stream()
                .map(s -> new WindowStat(
                        s.getId().getType().name(),
                        s.getId().getStart().toString(),
                        s.getId().getEnd().toString(),
                        s.getCount()))
                .toList();
    }

    public record TypeCount(String type, long total) {}

    public record WindowStat(String type, String windowStart, String windowEnd, long count) {}

    public record Summary(long totalEvents, String mostCommonType, List<TypeCount> byType) {}
}
