package ams.lawenforcement.repository;

import ams.lawenforcement.bolo.BoloLevel;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.Collection;
import java.util.List;
import java.util.Set;

@Repository
public interface LawEnforcementRepository extends CrudRepository<LawEnforcementAccident, Long> {

    /**
     * Highest persisted Kafka offset per partition for the given topic.
     * Used to skip records that were already consumed (idempotency).
     */
    @Query("select e.kafkaPartition as partition, max(e.kafkaOffset) as maxOffset " +
           "from LawEnforcementAccident e where e.kafkaTopic = :topic group by e.kafkaPartition")
    List<PartitionOffset> findHighWaterMarks(@Param("topic") String topic);

    /** Which of the given accident identities already exist — used to skip replayed records. */
    @Query("select e.cacheId from LawEnforcementAccident e where e.cacheId in :cacheIds")
    Set<String> findExistingCacheIds(@Param("cacheIds") Collection<String> cacheIds);

    /** Most recent active BOLO incidents (e.g. HIGH/CRITICAL), newest first. */
    List<LawEnforcementAccident> findTop50ByBoloLevelInOrderByIdDesc(BoloLevel... levels);

    /**
     * Crime counts per ~1&nbsp;km grid cell (coordinates rounded to 2 decimals) since {@code from},
     * busiest first. The regex guards against non-numeric coordinates so the cast can't fail.
     */
    @Query(value = """
            select round(cast(latitude as numeric), 2)  as cellLat,
                   round(cast(longitude as numeric), 2) as cellLng,
                   count(*)                              as total,
                   count(*) filter (where bolo_level <> 'NONE') as boloCount,
                   max(address)                          as sampleAddress
            from law_enforcement_accidents
            where accident_date >= :from
              and latitude  ~ '^-?[0-9]+(\\.[0-9]+)?$'
              and longitude ~ '^-?[0-9]+(\\.[0-9]+)?$'
            group by cellLat, cellLng
            order by total desc
            limit :limit
            """, nativeQuery = true)
    List<HotspotRow> hotspots(@Param("from") LocalDate from, @Param("limit") int limit);

    /** Daily crime counts per grid cell since {@code from} — the series the EWMA forecast runs on. */
    @Query(value = """
            select round(cast(latitude as numeric), 2)  as cellLat,
                   round(cast(longitude as numeric), 2) as cellLng,
                   accident_date                        as day,
                   count(*)                             as total
            from law_enforcement_accidents
            where accident_date >= :from
              and latitude  ~ '^-?[0-9]+(\\.[0-9]+)?$'
              and longitude ~ '^-?[0-9]+(\\.[0-9]+)?$'
            group by cellLat, cellLng, accident_date
            """, nativeQuery = true)
    List<DailyCellRow> dailyCellCounts(@Param("from") LocalDate from);

    interface PartitionOffset {
        int getPartition();
        long getMaxOffset();
    }

    interface HotspotRow {
        java.math.BigDecimal getCellLat();
        java.math.BigDecimal getCellLng();
        long getTotal();
        long getBoloCount();
        String getSampleAddress();
    }

    interface DailyCellRow {
        java.math.BigDecimal getCellLat();
        java.math.BigDecimal getCellLng();
        LocalDate getDay();
        long getTotal();
    }
}
