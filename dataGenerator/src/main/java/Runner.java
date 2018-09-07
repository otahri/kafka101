

import generator.EventGenerator;
import org.apache.avro.Schema;
import stream.EventStream;

import java.io.File;
import java.io.IOException;

public class Runner {
    public static void main(String[] args) throws IOException {

        Schema schema = null;
        try {
            schema = new Schema.Parser().parse(new File("Event.avsc"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        EventGenerator eventGenerator = new EventGenerator(schema);
        EventStream eventStream = new EventStream(eventGenerator);

        eventStream.getEvent()
                .forEach(genericRecord -> System.out.println(genericRecord));

    }
}
