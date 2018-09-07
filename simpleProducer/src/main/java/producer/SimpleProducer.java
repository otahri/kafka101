package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducer {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SimpleProducer.class);

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);


        try (KafkaProducer<String, String> kProducer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 100; i++) {
                 RecordMetadata rm =  kProducer.send(new ProducerRecord<>("test-topic", Integer.toString(i), Integer.toString(i))).get();
                System.out.println(rm.offset());
            }
        } catch (InterruptedException|ExecutionException e) {
            e.printStackTrace();
        }
    }
}
