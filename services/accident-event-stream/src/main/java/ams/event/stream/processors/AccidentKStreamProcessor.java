package ams.event.stream.processors;

import ams.data.model.AccidentEventModel;
import ams.event.stream.distributors.EventDistributor;
import ams.event.stream.distributors.delivery.FraudDetectionDelivery;
import ams.event.stream.distributors.delivery.SensitiveZoneDelivery;
import ams.event.stream.distributors.delivery.StatisticsEventDelivery;
import ams.event.stream.serde.AvroSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.*;
import org.springframework.stereotype.Component;

import static ams.data.model.AccidentType.*;

@Slf4j
@Component
@RequiredArgsConstructor
public class AccidentKStreamProcessor {
    private final EventDistributor distributor;
    private final StatisticsEventDelivery statisticsEventDelivery;
    private final SensitiveZoneDelivery sensitiveZoneDelivery;
    private final FraudDetectionDelivery fraudDetectionDelivery;

    public void process(KStream<String, AccidentEventModel> stream) {
        stream.split(Named.as("distribute"))
                .branch((key, accident) -> accident.getType().equals(CAR_ACCIDENT),
                        distributor.distribute(CAR_ACCIDENT)
                )
                .branch((key, accident) -> accident.getType().equals(FIRE_ACCIDENT),
                        distributor.distribute(FIRE_ACCIDENT)
                )
                .branch((key, accident) -> accident.getType().equals(CRIMINAL),
                        distributor.distribute(CRIMINAL)
                )
                .branch((key, accident) -> accident.getType().equals(OTHER_ACCIDENT),
                        distributor.distribute(OTHER_ACCIDENT)
                );

        /*
         * Since statistics service is independent of accident type,
         * it does not suffer from branched distribution
         * */
        statisticsEventDelivery.deliverEvent(stream);

        // Geo-fencing: incidents in sensitive zones are additionally routed to the sensitive topic.
        sensitiveZoneDelivery.deliverEvent(stream);

        // Fraud detection: locations reported too often in a short window raise a fraud alert.
        fraudDetectionDelivery.deliverEvent(stream);
    }
}
