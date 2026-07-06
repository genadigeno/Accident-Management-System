package ams.dispatch.api;

import ams.dispatch.domain.Dispatch;
import ams.dispatch.domain.DispatchStatus;
import ams.dispatch.domain.Unit;
import ams.dispatch.repository.DispatchRepository;
import ams.dispatch.repository.UnitRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.EnumSet;
import java.util.List;

/** Read API over the fleet and the dispatch log. */
@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class DispatchController {

    private final UnitRepository unitRepository;
    private final DispatchRepository dispatchRepository;

    /** The whole fleet with current state and position. */
    @GetMapping("/units")
    public List<UnitView> units() {
        return unitRepository.findAll().stream().map(UnitView::from).toList();
    }

    /** The 50 most recent dispatches, newest first. */
    @GetMapping("/dispatches")
    public List<DispatchView> dispatches() {
        return dispatchRepository.findTop50ByOrderByCreatedAtDesc().stream().map(DispatchView::from).toList();
    }

    /** Everything not yet cleared (waiting + in progress), newest first. */
    @GetMapping("/dispatches/active")
    public List<DispatchView> active() {
        return dispatchRepository.findByStatusInOrderByCreatedAtDesc(
                        EnumSet.of(DispatchStatus.WAITING, DispatchStatus.DISPATCHED,
                                DispatchStatus.EN_ROUTE, DispatchStatus.ON_SCENE))
                .stream().map(DispatchView::from).toList();
    }

    public record UnitView(String id, String type, String state, double latitude, double longitude) {
        static UnitView from(Unit unit) {
            return new UnitView(unit.getId(), unit.getType().name(), unit.getState().name(),
                    unit.getLatitude(), unit.getLongitude());
        }
    }

    public record DispatchView(String id, String incidentId, String unitType, String unitId,
                               String status, String address,
                               Instant createdAt, Instant dispatchedAt, Instant enRouteAt,
                               Instant onSceneAt, Instant clearedAt) {
        static DispatchView from(Dispatch dispatch) {
            return new DispatchView(dispatch.getId(), dispatch.getCacheId(),
                    dispatch.getUnitType().name(), dispatch.getUnitId(),
                    dispatch.getStatus().name(), dispatch.getAddress(),
                    dispatch.getCreatedAt(), dispatch.getDispatchedAt(), dispatch.getEnRouteAt(),
                    dispatch.getOnSceneAt(), dispatch.getClearedAt());
        }
    }
}
