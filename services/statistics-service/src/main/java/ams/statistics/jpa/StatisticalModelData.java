package ams.statistics.jpa;

import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.*;

@Table(name = "statistical_models", schema = "public")
@Entity
@Getter @Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StatisticalModelData {
    @EmbeddedId
    private WindowedId id;
    private long count;
}
