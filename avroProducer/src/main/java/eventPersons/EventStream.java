package eventPersons;

import kafka101.mydev.Event;

import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.NONNULL;

public class EventStream {
    public static Stream<Event> getEvent() {
        PersonIterator personIterator = new PersonIterator(new PersonGenerator());
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(personIterator, IMMUTABLE | NONNULL), false);
    }
}
