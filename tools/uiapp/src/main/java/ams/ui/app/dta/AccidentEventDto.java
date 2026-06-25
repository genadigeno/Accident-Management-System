package ams.ui.app.dta;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Lightweight view of an accident event pushed to the dashboard over WebSocket. */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AccidentEventDto {
    private String id;
    private String type;
    private String address;
    private String latitude;
    private String longitude;
    private String date;
    private long receivedAt;
}
