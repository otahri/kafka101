package personAvroProducer;

import eventPersons.PersonGenerator;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import kafka101.mydev.Event;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static net.sourceforge.argparse4j.impl.Arguments.store;

public class AvroProducerSSL {
    public static void main(String[] args) {

        ArgumentParser parser = argParser();

        try {
            Namespace res = parser.parseArgs(args);

            /* parse args */
            String brokerList = res.getString("bootstrap.servers");
            String topic = res.getString("topic");
            String jksKeystore = res.getString("jksKeystore");
            String jksTrustore = res.getString("jksTrustore");
            String jksPassword = res.getString("jksPassword");
            String schemaRegistryUrl = res.getString("schema.registry.url");


            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.RETRIES_CONFIG, 0);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
            props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

            System.setProperty("javax.net.ssl.trustStore", jksTrustore);
            System.setProperty("javax.net.ssl.trustStorePassword", jksPassword);
            System.setProperty("javax.net.ssl.keyStore", jksKeystore);
            System.setProperty("javax.net.ssl.keyStorePassword", jksPassword);

            // configure the following three settings for SSL Encryption
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, jksTrustore);
            props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, jksPassword);

            // configure the following three settings for SSL Authentication
            props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, jksKeystore);
            props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, jksPassword);
            props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, jksPassword);

            PersonGenerator personGenerator = new PersonGenerator();

            try (KafkaProducer<String, Event> kProducer = new KafkaProducer<>(props)) {
                for (int i = 0; i < 10; i++) {
                    Event event = personGenerator.generate();
                    kProducer.send(new ProducerRecord<>(topic, event.getNom().toString(), event), ((RecordMetadata metadata, Exception exception) -> {
                                if (exception != null) {
                                    exception.printStackTrace();
                                } else {
                                    System.out.format("Offset: %d, Partition: %d, Timestamp: %d%n ", metadata.offset(), metadata.partition(), metadata.timestamp());
                                    System.out.format("Name: %s, FirstName: %s, City: %s%n ", event.getNom(), event.getPrenom(), event.getVille());
                                }
                            })
                    );
                }
            }

        } catch (ArgumentParserException e) {
            if (args.length == 0) {
                parser.printHelp();
                System.exit(0);
            } else {
                parser.handleError(e);
                System.exit(1);
            }
        }
    }

    private static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("simpleProducerSSL")
                .defaultHelp(true)
                .description("This is to test kafka producer");

        parser.addArgument("--bootstrap.servers").action(store())
                .required(true)
                .type(String.class)
                .metavar("BROKER-LIST")
                .help("comma separated broker list followed by port");

        parser.addArgument("--schema.registry.url").action(store())
                .required(true)
                .type(String.class)
                .metavar("SCHEMA-REGISTRY")
                .help("Schema Registry URL followed by port");

        parser.addArgument("--topic").action(store())
                .required(true)
                .type(String.class)
                .metavar("TOPIC")
                .help("topic name");

        parser.addArgument("--jksKeystore").action(store())
                .required(true)
                .type(String.class)
                .metavar("JKSKEYSTORE")
                .help("Jks certificate keystore");

        parser.addArgument("--jksTrustore").action(store())
                .required(true)
                .type(String.class)
                .metavar("JKSTRUSTSTORE")
                .help("Jks certificate trustore");

        parser.addArgument("--jksPassword").action(store())
                .required(true)
                .type(String.class)
                .metavar("JKSPASSWORD")
                .help("Jks certificate password");

        return parser;
    }
}

