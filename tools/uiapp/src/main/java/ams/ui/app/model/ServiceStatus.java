package ams.ui.app.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ServiceStatus {
    public enum Status {
        UP,
        DEGRADED,
        DOWN,
        UNKNOWN
    }

    private Status status;
}
