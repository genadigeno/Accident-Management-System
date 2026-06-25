package ams.ui.app.controller;

import ams.ui.app.dta.MessageRequest;
import ams.ui.app.dta.SendBatchView;
import ams.ui.app.model.SendBatch;
import ams.ui.app.service.MessageService;
import ams.ui.app.service.SendBatchRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1/messages")
@RequiredArgsConstructor
public class MessageRestController {

    private final MessageService messageService;
    private final SendBatchRegistry batchRegistry;

    /** Kicks off an async batch; returns 202 with the batch so the client can track it. */
    @PostMapping
    public ResponseEntity<SendBatchView> sendMessage(MessageRequest messageRequest) {
        SendBatch batch = messageService.send(messageRequest);
        return ResponseEntity.accepted().body(SendBatchView.from(batch));
    }

    /** The most recent send batches, newest first. */
    @GetMapping("/batches")
    public List<SendBatchView> batches() {
        return batchRegistry.recent();
    }

    @GetMapping("/batches/{id}")
    public ResponseEntity<SendBatchView> batch(@PathVariable String id) {
        return batchRegistry.get(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}
