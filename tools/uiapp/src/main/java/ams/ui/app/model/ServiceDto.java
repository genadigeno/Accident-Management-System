package ams.ui.app.model;

import lombok.Data;

@Data
public class ServiceDto {
    private String serviceName;
    private String serviceUrl;
    private String serviceVersion;
    private String serviceDescription;
    private String serviceId;
    private int port;
}
