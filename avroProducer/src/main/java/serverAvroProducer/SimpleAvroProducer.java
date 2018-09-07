package serverAvroProducer;

import eventServers.EventGenerator;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import kafka101.mydev.LogEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SimpleAvroProducer {

    private final static Logger LOG = LoggerFactory.getLogger(SimpleAvroProducer.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        final String TOPIC_NAME = "topic_avro";
        EventGenerator eventGenerator = new EventGenerator();

        try (KafkaProducer<String, LogEvent> kProducer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 10; i++) {
                LogEvent logEvent = eventGenerator.generate();
                kProducer.send(new ProducerRecord<>(TOPIC_NAME, logEvent.getServer().toString(), logEvent), ((RecordMetadata metadata, Exception exception) -> {
                            if (exception != null) {
                                exception.printStackTrace();
                            } else {
                                System.out.format("Offset: %d, Partition: %d, Timestamp: %d%n ", metadata.offset(), metadata.partition(), metadata.timestamp());
                                //System.out.format("Server: %s, Duration: %s, Name: %s%n ", logEvent.getServer(), logEvent.getDuration(), logEvent.getName());
                            }
                        })
                );
            }
        }
    }
}
