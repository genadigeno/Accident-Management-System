package ams.ui.app.dta;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** A dashboard alert pushed over WebSocket ({@code /topic/alerts}). */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AlertDto {
    /** BOLO / SLA / FRAUD / GEOFENCE */
    private String source;
    /** CRITICAL / HIGH / MEDIUM / INFO */
    private String severity;
    private String title;
    private String message;
    private long at;
}
