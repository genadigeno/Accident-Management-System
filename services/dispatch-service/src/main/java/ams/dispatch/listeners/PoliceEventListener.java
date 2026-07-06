package ams.dispatch.listeners;

import ams.data.model.PoliceEventModel;
import ams.data.model.UnitType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

/** Every police event needs a patrol car. */
@Slf4j
@Component
@RequiredArgsConstructor
public class PoliceEventListener {

    private final DispatchProcessor processor;

    @KafkaListener(
            topics = "${kafka.topic.law-enforcement}",
            containerFactory = "kafkaListenerContainerFactory",
            concurrency = "1",
            batch = "true"
    )
    public void handle(List<ConsumerRecord<String, PoliceEventModel>> records, Acknowledgment ack) {
        processor.process(records, UnitType.POLICE_CAR, event -> DispatchProcessor.toIncident(
                event.getCacheId(), event.getAddress(), event.getLatitude(), event.getLongitude()));
        ack.acknowledge();
    }
}
