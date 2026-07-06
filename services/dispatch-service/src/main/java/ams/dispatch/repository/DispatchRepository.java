package ams.dispatch.repository;

import ams.data.model.UnitType;
import ams.dispatch.domain.Dispatch;
import ams.dispatch.domain.DispatchStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.Collection;
import java.util.List;

@Repository
public interface DispatchRepository extends JpaRepository<Dispatch, String> {

    /** Idempotency: one dispatch per (incident, unit type), no matter how often the event is redelivered. */
    boolean existsByCacheIdAndUnitType(String cacheId, UnitType unitType);

    /** Call stacking queue, oldest first. */
    List<Dispatch> findTop50ByStatusOrderByCreatedAtAsc(DispatchStatus status);

    /** Active dispatches the simulator should advance now. */
    List<Dispatch> findByStatusInAndNextTransitionAtBefore(Collection<DispatchStatus> statuses, Instant now);

    List<Dispatch> findTop50ByOrderByCreatedAtDesc();

    List<Dispatch> findByStatusInOrderByCreatedAtDesc(Collection<DispatchStatus> statuses);
}
