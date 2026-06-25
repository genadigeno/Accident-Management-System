package ams.event.stream.distributors.delivery;

import ams.data.model.AccidentEventModel;
import ams.data.model.EmergencyEventModel;
import ams.event.stream.serde.AvroSerde;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class EmergencyEventDelivery implements EventDelivery<String, AccidentEventModel> {
    @Value("${topic.config.emergency}")
    private String emergencyTopic;
    @Value("${spring.kafka.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Override
    public void deliverEvent(KStream<String, AccidentEventModel> ks) {
        ks.mapValues((key, accident) ->
                        EmergencyEventModel.newBuilder()
                                .setAddress(accident.getLocation().getAddress())
                                .setLatitude(accident.getLocation().getLatitude())
                                .setLongitude(accident.getLocation().getLongitude())
                                .setCacheId(accident.getCacheId())
                                .setDate(accident.getDate())
                                .setId(accident.getId())
                                .setDescription(accident.getDescription())
                                .build())
                .to(emergencyTopic, Produced.with(AvroSerde.String(schemaRegistryUrl),
                        AvroSerde.EmergencyEventModel(schemaRegistryUrl)));
    }
}
