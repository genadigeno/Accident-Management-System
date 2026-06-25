package ams.event.stream.handler;

import ams.data.model.DeserializationErrorResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;

import java.time.LocalDate;


@Slf4j
@RequiredArgsConstructor
public class AccidentDeserializationErrorRecoverer implements ConsumerRecordRecoverer {
    private final KafkaTemplate<String, DeserializationErrorResponse> kafkaTemplate;
    private final String dltTopic;

    @Override
    public void accept(ConsumerRecord<?, ?> consumerRecord, Exception exception) {
        log.warn("Building `DeserializationErrorResponse` class, key : {}, value : {}",
                consumerRecord.key().getClass(), consumerRecord.value().getClass());

        DeserializationErrorResponse data = DeserializationErrorResponse.newBuilder()
                .setDate(LocalDate.now())
                .setKey(consumerRecord.key().toString())
                .setValue(consumerRecord.value().toString())
                .setDescription("corrupted data")
                .build();

        log.warn("Sending data {}, to dead letter topic : {}", data, dltTopic);

        ProducerRecord<String, DeserializationErrorResponse> producerRecord =
                new ProducerRecord<>(dltTopic, 0, consumerRecord.key().toString(), data);
        kafkaTemplate.send(producerRecord);
    }
}
