package stream;


import generator.EventGenerator;
import org.apache.avro.generic.GenericRecord;

import java.util.Iterator;

final class EventIterator implements Iterator<GenericRecord> {

    private EventGenerator eventGenerator;

    public EventIterator(EventGenerator eventGenerator) {
        this.eventGenerator = eventGenerator;
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public GenericRecord next() {
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return eventGenerator.generate();
    }
}
