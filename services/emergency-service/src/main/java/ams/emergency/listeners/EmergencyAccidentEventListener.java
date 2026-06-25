package ams.emergency.listeners;

import ams.data.model.EmergencyEventModel;
import ams.emergency.processors.EmergencyEventProcessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class EmergencyAccidentEventListener {
    private final EmergencyEventProcessor eventProcessor;

    /*
     * containerFactory - custom ConcurrentKafkaListenerContainerFactory<?,?> class
     * concurrency - why use only 1 thread, therefore it provides a possibility of vertical scaling
     * */
    @KafkaListener(
            topics="${kafka.main.topic}",
            containerFactory="kafkaListenerContainerFactory",
            concurrency="1",
            batch = "true"
    )
    public void handler(List<ConsumerRecord<String, EmergencyEventModel>> messages, Acknowledgment ack) {
        try {
            log.info("Starting the process to receive batch messages");
            eventProcessor.process(messages);

            //Manual acknowledgement
            ack.acknowledge();
            log.info("all the batch messages are consumed");
        } catch (Exception ex) {
            log.error("EmergencyAccidentEventListener Error - {}", ex.getMessage(), ex);
            throw ex;
        }
    }
}
