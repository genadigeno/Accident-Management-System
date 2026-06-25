package ams.event.stream.distributors.delivery;

import ams.data.model.AccidentEventModel;
import ams.event.stream.geofence.GeoFenceClassifier;
import ams.event.stream.serde.AvroSerde;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Geo-fencing sink: incidents located in a configured sensitive zone are routed to an extra
 * "sensitive" topic (in addition to their normal responder routing) for special handling,
 * and raise a {@code ams.geofence.sensitive} metric plus a WARN alert.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SensitiveZoneDelivery implements EventDelivery<String, AccidentEventModel> {

    @Value("${topic.config.sensitive}")
    private String sensitiveTopic;
    @Value("${spring.kafka.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    private final GeoFenceClassifier geoFenceClassifier;
    private final MeterRegistry meterRegistry;

    @Override
    public void deliverEvent(KStream<String, AccidentEventModel> ks) {
        ks.filter((key, accident) -> geoFenceClassifier.isSensitive(addressOf(accident)))
                .peek((key, accident) -> {
                    meterRegistry.counter("ams.geofence.sensitive").increment();
                    log.warn("Sensitive-zone incident: type={}, address='{}'", accident.getType(), addressOf(accident));
                })
                .to(sensitiveTopic, Produced.with(
                        AvroSerde.String(schemaRegistryUrl),
                        AvroSerde.AccidentEventModel(schemaRegistryUrl)
                ));
    }

    private static String addressOf(AccidentEventModel accident) {
        if (accident.getLocation() == null || accident.getLocation().getAddress() == null) {
            return null;
        }
        return accident.getLocation().getAddress().toString();
    }
}
