package ams.lawenforcement.repository;

import ams.lawenforcement.bolo.BoloLevel;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

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

    interface PartitionOffset {
        int getPartition();
        long getMaxOffset();
    }
}
