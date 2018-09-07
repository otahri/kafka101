package eventServers;

import kafka101.mydev.LogEvent;

import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.NONNULL;

public class EventStream {
    public static Stream<LogEvent> getEvent(){
        EventIterator eventIterator = new EventIterator(new EventGenerator());
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(eventIterator, IMMUTABLE | NONNULL), false);
    }
}
