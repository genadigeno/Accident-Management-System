package ams.event.stream.distributors;

import ams.data.model.AccidentEventModel;
import ams.data.model.AccidentType;
import ams.event.stream.distributors.delivery.EmergencyEventDelivery;
import ams.event.stream.distributors.delivery.FireEventDelivery;
import ams.event.stream.distributors.delivery.PoliceEventDelivery;
import ams.event.stream.exception.InvalidEventException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.Branched;
import org.springframework.stereotype.Component;

import static ams.data.model.AccidentType.*;
import static ams.data.model.AccidentType.OTHER_ACCIDENT;

@Slf4j
@Component
@RequiredArgsConstructor
public class EventDistributor {
    private final PoliceEventDelivery policeEventDelivery;
    private final EmergencyEventDelivery emergencyEventDelivery;
    private final FireEventDelivery fireEventDelivery;

    public Branched<String, AccidentEventModel> distribute(AccidentType type) {
        if (type == CAR_ACCIDENT || type == FIRE_ACCIDENT) {
            return Branched.withConsumer(ks -> {
                policeEventDelivery.deliverEvent(ks);
                emergencyEventDelivery.deliverEvent(ks);
                fireEventDelivery.deliverEvent(ks);
            });
        }
        if (type == CRIMINAL) {
            return Branched.withConsumer(ks -> {
                policeEventDelivery.deliverEvent(ks);
                emergencyEventDelivery.deliverEvent(ks);
            });
        }
        if (type == OTHER_ACCIDENT) {
            return Branched.withConsumer(policeEventDelivery::deliverEvent);
        }
        else throw new InvalidEventException();
    }
}
