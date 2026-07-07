package ams.correlation.domain;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface IncidentReportRepository extends CrudRepository<IncidentReport, String> {

    List<IncidentReport> findByIncidentIdOrderByReportedAtAsc(String incidentId);
}
