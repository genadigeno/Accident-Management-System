package ams.correlation.domain;

import ams.data.model.AccidentType;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
public interface IncidentRepository extends JpaRepository<Incident, String> {

    /**
     * Open incidents of the same type in the 3x3 grid-cell neighbourhood that are still inside
     * the correlation window — the merge candidates for a new report. Locked so the listener
     * and the auto-close scheduler cannot race on the same incident.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("""
            select i from Incident i
            where i.state = :state
              and i.accidentType = :type
              and i.cellLat between :latLo and :latHi
              and i.cellLng between :lngLo and :lngHi
              and i.lastReportedAt >= :since
            order by i.lastReportedAt desc
            """)
    List<Incident> lockCandidates(@Param("state") Incident.State state,
                                  @Param("type") AccidentType type,
                                  @Param("latLo") int latLo, @Param("latHi") int latHi,
                                  @Param("lngLo") int lngLo, @Param("lngHi") int lngHi,
                                  @Param("since") Instant since);

    /** Read-only variant for the gateway's duplicate hint. */
    @Query("""
            select i from Incident i
            where i.state = :state
              and i.accidentType = :type
              and i.cellLat between :latLo and :latHi
              and i.cellLng between :lngLo and :lngHi
              and i.lastReportedAt >= :since
            order by i.lastReportedAt desc
            """)
    List<Incident> findCandidates(@Param("state") Incident.State state,
                                  @Param("type") AccidentType type,
                                  @Param("latLo") int latLo, @Param("latHi") int latHi,
                                  @Param("lngLo") int lngLo, @Param("lngHi") int lngHi,
                                  @Param("since") Instant since);

    List<Incident> findTop50ByOrderByLastReportedAtDesc();

    List<Incident> findTop50ByStateAndLastReportedAtBefore(Incident.State state, Instant before);
}
