package ams.app;

import ams.app.util.LocationGenerator;
import ams.data.model.AccidentEventModel;
import ams.data.model.AccidentType;
import ams.data.model.Location;

import java.time.LocalDate;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/** Builds random {@link AccidentEventModel} events for load testing. */
final class EventGenerator {

    private static final AccidentType[] TYPES = AccidentType.values();

    private EventGenerator() {
    }

    static AccidentEventModel next() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        return AccidentEventModel.newBuilder()
                .setId(random.nextInt(1_000_000))
                .setType(TYPES[random.nextInt(TYPES.length)])
                .setDate(LocalDate.now())
                .setDescription(randomDescription())
                .setCacheId(UUID.randomUUID().toString())
                .setLocationBuilder(Location.newBuilder()
                        .setAddress(LocationGenerator.generateAddress())
                        .setLatitude(LocationGenerator.generateLatitude())
                        .setLongitude(LocationGenerator.generateLongitude()))
                .build();
    }

    private static String randomDescription() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int length = 10 + random.nextInt(16);   // 10–25 letters
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append((char) ('a' + random.nextInt(26)));
        }
        return sb.toString();
    }
}
