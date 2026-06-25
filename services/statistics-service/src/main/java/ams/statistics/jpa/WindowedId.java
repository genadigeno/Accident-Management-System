package ams.statistics.jpa;

import ams.data.model.AccidentType;
import lombok.*;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import java.io.Serializable;
import java.time.LocalDateTime;

@Getter @Setter
@NoArgsConstructor
@AllArgsConstructor
@Embeddable
@Builder
public class WindowedId implements Serializable {
    @Column(name = "window_end")
    private LocalDateTime end;
    @Column(name = "window_start")
    private LocalDateTime start;
    @Enumerated(EnumType.STRING)
    private AccidentType type;
}
