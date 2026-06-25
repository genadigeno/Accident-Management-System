package ams.ui.app.util;

import java.util.Random;

public final class LocationPropertyGenerator {
    private static final Random RANDOM = new Random();
    private static final String[] STREET_PREFIXES = new String[]{"North", "South", "East", "West"};
    private static final String[] STREET_NAMES = new String[]{"Maple", "Oak", "Cedar", "Pine"};
    private static final String[] STREET_TYPES = new String[]{"Street", "Avenue", "Boulevard", "Lane"};
    private static final String[] CITIES = new String[]{"Cityville", "Townsville", "Villagetown", "Hamlet"};

    private LocationPropertyGenerator() {
    }

    public static String generateLongitude() {
        double min = -180.0;
        double max = 180.0;
        return Double.toString(min + (max - min) * RANDOM.nextDouble());
    }

    public static String generateLatitude() {
        double min = -90.0;
        double max = 90.0;
        return Double.toString(min + (max - min) * RANDOM.nextDouble());
    }

    public static String generateAddress() {
        String streetPrefix = getRandomElement(STREET_PREFIXES);
        String streetName = getRandomElement(STREET_NAMES);
        String streetType = getRandomElement(STREET_TYPES);
        String city = getRandomElement(CITIES);
        int randomHouseNumber = RANDOM.nextInt(1000) + 1;
        return randomHouseNumber + " " + streetPrefix + " " + streetName + " " + streetType + ", " + city;
    }

    private static String getRandomElement(String[] array) {
        int randomIndex = RANDOM.nextInt(array.length);
        return array[randomIndex];
    }
}
