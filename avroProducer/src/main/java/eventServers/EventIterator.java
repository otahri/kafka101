package eventServers;

import kafka101.mydev.LogEvent;

import java.util.Iterator;

final class EventIterator implements Iterator<LogEvent> {

    private final EventGenerator eventGenerator;

    public EventIterator(EventGenerator eventGenerator) {
        this.eventGenerator = eventGenerator;
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public LogEvent next() {
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return eventGenerator.generate();
    }
}
