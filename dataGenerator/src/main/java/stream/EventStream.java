package stream;

import generator.EventGenerator;
import org.apache.avro.generic.GenericRecord;

import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.NONNULL;

public class EventStream {

    private EventGenerator eventGenerator;

    public EventStream(EventGenerator eventGenerator) {
        this.eventGenerator = eventGenerator;
    }

    public Stream<GenericRecord> getEvent() {
        EventIterator eventIterator = new EventIterator(eventGenerator);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(eventIterator, IMMUTABLE | NONNULL), false);
    }
}
