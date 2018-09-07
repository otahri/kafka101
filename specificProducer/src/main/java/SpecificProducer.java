import com.opencsv.CSVReader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class SpecificProducer {
    public static void main(String[] args) throws IOException {
        final String TOPIC_NAME = "topic_specific";
        final String SRC_FILE = "src_file";
        final String TRACK_FILE = "track_" + SRC_FILE;
        FileReader srcFile = new FileReader(SRC_FILE + ".csv");
        String[] myLine;
        CSVReader csvReader;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG,"all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class);

        if (Files.exists(Paths.get(TRACK_FILE))){
            int line = Integer.parseInt(Files.readAllLines(Paths.get(TRACK_FILE),Charset.defaultCharset()).get(0));
            csvReader = new CSVReader(srcFile,',',' ',line);
        } else {
            csvReader = new CSVReader(srcFile,',');
        }

        while ((myLine = csvReader.readNext()) != null){
            try(KafkaProducer<String, String> kProducer = new KafkaProducer<>(props)) {

                String key = myLine[0];
                String value = myLine[1];
                Long lineNumb = csvReader.getLinesRead();

                kProducer.send(new ProducerRecord<>(TOPIC_NAME, key, value), ((RecordMetadata metadata, Exception exception) -> {
                    if (exception != null) {
                        exception.printStackTrace();
                    } else {
                        System.out.format("Offset: %d, Key: %s, Value: %s, LineNumber: %d %n ", metadata.offset(), key, value, lineNumb);
                        try {
                            Files.write(Paths.get(TRACK_FILE), lineNumb.toString().getBytes());
                            Thread.sleep(500);
                        } catch (IOException|InterruptedException e) {
                            e.printStackTrace();
                        }
                    } })
                );
            }
        }
    }
}

