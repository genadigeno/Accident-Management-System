package ams.notification.domain;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface NotificationRepository extends CrudRepository<Notification, Long> {

    boolean existsByDedupKey(String dedupKey);

    List<Notification> findTop50ByOrderByIdDesc();

    @Query("""
            select n.source as source, n.severity as severity, count(n) as total
            from Notification n
            group by n.source, n.severity
            order by count(n) desc
            """)
    List<SourceSeverityCount> countBySourceAndSeverity();

    interface SourceSeverityCount {
        String getSource();
        String getSeverity();
        long getTotal();
    }
}
