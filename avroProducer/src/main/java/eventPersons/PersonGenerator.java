package eventPersons;

import kafka101.mydev.Event;

import java.util.Random;

public class PersonGenerator {
    private final static String[] NAMES = {
            "Name1",
            "Name2",
            "Name3",
            "Name4"};

    private final static String[] FIRSTNAMES = {
            "FistName1",
            "FistName2",
            "FistName3",
            "FistName4"};

    private final static String[] CITIES = {
            "City1",
            "City2",
            "City3",
            "City4"};

    private final Random random = new Random();

    public Event generate() {
        Event.Builder logEventBuilder = Event.newBuilder()
                .setNom(pickName())
                .setPrenom(pickFirstName())
                .setVille(pickCity());

        return logEventBuilder.build();
    }

    private String pickName() {
        return NAMES[random.nextInt(NAMES.length)];
    }

    private String pickFirstName() {
        return FIRSTNAMES[random.nextInt(FIRSTNAMES.length)];
    }

    private String pickCity() {
        return CITIES[random.nextInt(CITIES.length)];
    }

}
