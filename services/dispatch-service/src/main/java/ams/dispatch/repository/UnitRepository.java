package ams.dispatch.repository;

import ams.data.model.UnitType;
import ams.dispatch.domain.Unit;
import ams.dispatch.domain.UnitState;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface UnitRepository extends JpaRepository<Unit, String> {

    /**
     * All free units of a type, locked for update: the assignment scheduler and the Kafka
     * listeners must never hand the same unit to two incidents.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select u from Unit u where u.type = :type and u.state = :state")
    List<Unit> lockByTypeAndState(@Param("type") UnitType type, @Param("state") UnitState state);
}
