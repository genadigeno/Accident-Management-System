package ams.ui.app.dta;

import ams.data.model.AccidentType;
import ams.data.model.Location;
import lombok.Data;

@Data
public class MessageDto {
    private String latitude;
    private String longitude;
    private AccidentType type;
    private Location location;
    private String description;
    private String cacheId;
}
