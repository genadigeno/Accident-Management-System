package ams.ui.app.controller;

import ams.ui.app.dta.ServiceHealthView;
import ams.ui.app.model.ServiceDto;
import ams.ui.app.model.ServiceStatus;
import ams.ui.app.model.StatusCheck;
import ams.ui.app.service.RegistratorService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/api/v1/register")
@RequiredArgsConstructor
public class RegistratorController {
    private final RegistratorService registratorService;

    /** Current health snapshot of all monitored services (used for the dashboard's first paint). */
    @GetMapping
    public List<ServiceHealthView> services() {
        return registratorService.snapshot();
    }

    @PostMapping()
    public ResponseEntity<Void> register(@RequestBody ServiceDto serviceDto) {
        log.info("Received registration request: {}", serviceDto);
        registratorService.registerService(serviceDto);
        return  ResponseEntity.ok().build();
    }

    @GetMapping("/{serviceName}")
    public ResponseEntity<String> receiveHeartBeat(@PathVariable String serviceName) {
        log.info("Received heart beat from: {}", serviceName);
        registratorService.receiveHeartBeat(serviceName);
        return ResponseEntity.ok().build();
    }

    @MessageMapping("/status-check")
    @SendTo("/topic/service-discovery")
    public ResponseEntity<ServiceStatus> checkServiceStatus(StatusCheck statusCheck) {
        log.info("Received status check: {}", statusCheck);

        return ResponseEntity.ok(ServiceStatus.builder()
                .status(ServiceStatus.Status.UP)
                .build()
        );
    }
}
