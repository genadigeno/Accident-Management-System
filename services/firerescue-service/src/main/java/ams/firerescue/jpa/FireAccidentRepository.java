package ams.firerescue.jpa;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface FireAccidentRepository extends CrudRepository<FireAccident, Long> {

    /**
     * Highest persisted Kafka offset per partition for the given topic.
     * Used to skip records that were already consumed (idempotency).
     */
    @Query("select e.kafkaPartition as partition, max(e.kafkaOffset) as maxOffset " +
           "from FireAccident e where e.kafkaTopic = :topic group by e.kafkaPartition")
    List<PartitionOffset> findHighWaterMarks(@Param("topic") String topic);

    interface PartitionOffset {
        int getPartition();
        long getMaxOffset();
    }
}
