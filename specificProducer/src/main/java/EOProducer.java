import com.opencsv.CSVReader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class EOProducer {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(EOProducer.class);

    public static void main(String[] args) throws IOException {

        final String TOPIC_NAME = "eo-topic-1p";
        final String SRC_FILE = "src_file";
        FileReader srcFile = new FileReader(SRC_FILE + ".csv");
        String[] myLine;
        CSVReader csvReader;
        LineTracker lineTracker = new LineTracker();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG,"all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class);

        int line = lineTracker.getLineTracker(SRC_FILE);
        csvReader = new CSVReader(srcFile,',',' ',line);

        while ((myLine = csvReader.readNext()) != null){
            try(KafkaProducer<String, String> kProducer = new KafkaProducer<>(props)) {

                String key = SRC_FILE + "_" + csvReader.getLinesRead();
                String value = myLine[1];
                Long lineNumb = csvReader.getLinesRead();

                logger.info("Start sending Line {}", csvReader.getLinesRead());


                kProducer.send(new ProducerRecord<>(TOPIC_NAME, key, value), ((RecordMetadata metadata, Exception exception) -> {
                    if (exception != null) {
                        exception.printStackTrace();
                    } else {
                        System.out.format("Partition : %d, Offset: %d, Key: %s, Value: %s, LineNumber: %d %n ", metadata.partition() ,metadata.offset(), key, value, lineNumb);
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    } })
                );
            }
        }
    }
}


