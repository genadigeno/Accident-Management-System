package ams.statistics.jpa;

import ams.data.model.AccidentType;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface StatisticalModelRepository extends CrudRepository<StatisticalModelData, Long> {

    // Total event count per accident type, most frequent first
    @Query("select s.id.type as type, sum(s.count) as total " +
           "from StatisticalModelData s group by s.id.type order by sum(s.count) desc")
    List<TypeTotal> totalsByType();

    // Total number of events aggregated across all windows
    @Query("select coalesce(sum(s.count), 0) from StatisticalModelData s")
    long totalEvents();

    // Most recent windowed aggregates, newest window first
    @Query("select s from StatisticalModelData s order by s.id.end desc")
    List<StatisticalModelData> findRecentWindows(Pageable pageable);

    interface TypeTotal {
        AccidentType getType();
        long getTotal();
    }
}
