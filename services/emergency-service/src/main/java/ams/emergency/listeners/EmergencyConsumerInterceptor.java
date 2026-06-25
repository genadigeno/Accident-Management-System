package ams.emergency.listeners;

import ams.data.model.EmergencyEventModel;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class EmergencyConsumerInterceptor implements ConsumerInterceptor<String, EmergencyEventModel> {
    private static final Logger log = LoggerFactory.getLogger(EmergencyConsumerInterceptor.class);

    @Override
    public ConsumerRecords<String, EmergencyEventModel> onConsume(ConsumerRecords<String, EmergencyEventModel> records) {
        return records;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {
        log.info("[COMMIT]: committing offset");
    }

    @Override
    public void close() {
        //
    }

    @Override
    public void configure(Map<String, ?> map) {
        //
    }
}
