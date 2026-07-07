package ams.dispatch.listeners;

import ams.data.model.EmergencyEventModel;
import ams.data.model.UnitType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

/** Every emergency event needs an ambulance. */
@Slf4j
@Component
@RequiredArgsConstructor
public class EmergencyEventListener {

    private final DispatchProcessor processor;

    @KafkaListener(
            topics = "${kafka.topic.emergency}",
            containerFactory = "kafkaListenerContainerFactory",
            concurrency = "${LISTENER_CONCURRENCY:3}",
            batch = "true"
    )
    public void handle(List<ConsumerRecord<String, EmergencyEventModel>> records, Acknowledgment ack) {
        processor.process(records, UnitType.AMBULANCE, event -> DispatchProcessor.toIncident(
                event.getCacheId(), event.getAddress(), event.getLatitude(), event.getLongitude()));
        ack.acknowledge();
    }
}
