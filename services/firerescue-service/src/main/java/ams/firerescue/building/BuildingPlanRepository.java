package ams.firerescue.building;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface BuildingPlanRepository extends JpaRepository<BuildingPlanEntity, Long> {
    Optional<BuildingPlanEntity> findByAddressKey(String addressKey);
}
