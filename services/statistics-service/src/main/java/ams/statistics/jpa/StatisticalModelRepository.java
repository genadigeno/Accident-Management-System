package ams.statistics.jpa;

import ams.data.model.AccidentType;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface StatisticalModelRepository extends CrudRepository<StatisticalModelData, Long> {

    // Total event count per accident type, most frequent first
    @Query("select s.id.type as type, sum(s.count) as total " +
           "from StatisticalModelData s group by s.id.type order by sum(s.count) desc")
    List<TypeTotal> totalsByType();

    // Total number of events aggregated across all windows
    @Query("select coalesce(sum(s.count), 0) from StatisticalModelData s")
    long totalEvents();

    // Most recent windowed aggregates, newest window first
    @Query("select s from StatisticalModelData s order by s.id.end desc")
    List<StatisticalModelData> findRecentWindows(Pageable pageable);

    /**
     * Rollup: events per time bucket and type since {@code from}. {@code unit} is a
     * {@code date_trunc} field name and is whitelisted by the service (hour/day/week).
     */
    @Query(value = """
            select date_trunc(:unit, window_start) as bucket, type, sum(count) as total
            from statistical_models
            where window_start >= :from
            group by bucket, type
            order by bucket desc, total desc
            """, nativeQuery = true)
    List<RollupRow> rollup(@Param("unit") String unit, @Param("from") LocalDateTime from);

    /** Total events whose window started inside [from, to) — for period-over-period trends. */
    @Query(value = """
            select coalesce(sum(count), 0)
            from statistical_models
            where window_start >= :from and window_start < :to
            """, nativeQuery = true)
    long totalBetween(@Param("from") LocalDateTime from, @Param("to") LocalDateTime to);

    /** Per-type totals inside [from, to). */
    @Query(value = """
            select type, sum(count) as total
            from statistical_models
            where window_start >= :from and window_start < :to
            group by type
            """, nativeQuery = true)
    List<TypeTotalRow> totalsByTypeBetween(@Param("from") LocalDateTime from, @Param("to") LocalDateTime to);

    /** Events per hour of day across all history — "peak accident hours: 5 PM – 7 PM". */
    @Query(value = """
            select cast(extract(hour from window_start) as int) as hour, sum(count) as total
            from statistical_models
            group by hour
            order by total desc
            """, nativeQuery = true)
    List<HourTotalRow> totalsByHourOfDay();

    interface TypeTotal {
        AccidentType getType();
        long getTotal();
    }

    interface RollupRow {
        Timestamp getBucket();
        String getType();
        Long getTotal();
    }

    interface TypeTotalRow {
        String getType();
        Long getTotal();
    }

    interface HourTotalRow {
        Integer getHour();
        Long getTotal();
    }
}
