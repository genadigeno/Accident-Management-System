package ams.emergency.response;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ResponseTimeRepository extends CrudRepository<ResponseTime, String> {

    List<ResponseTime> findTop50ByOrderByDispatchedAtDesc();

    /** Per-unit-type aggregates over completed responses (unit arrived on scene). */
    @Query(value = """
            select unit_type                                            as unitType,
                   count(*)                                             as total,
                   count(*) filter (where response_seconds is not null) as completed,
                   coalesce(sum(response_seconds), 0)                   as sumSeconds,
                   avg(response_seconds)                                as avgSeconds,
                   min(response_seconds)                                as minSeconds,
                   max(response_seconds)                                as maxSeconds,
                   count(*) filter (where sla_breached)                 as breached
            from response_times
            group by unit_type
            """, nativeQuery = true)
    List<UnitTypeStats> statsByUnitType();

    interface UnitTypeStats {
        String getUnitType();
        long getTotal();
        long getCompleted();
        long getSumSeconds();
        Double getAvgSeconds();
        Long getMinSeconds();
        Long getMaxSeconds();
        long getBreached();
    }
}
