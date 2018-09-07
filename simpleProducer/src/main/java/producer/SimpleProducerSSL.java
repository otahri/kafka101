package producer;

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
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static net.sourceforge.argparse4j.impl.Arguments.store;

public class SimpleProducerSSL {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SimpleProducerSSL.class);

    public static void main(String[] args) {

        ArgumentParser parser = argParser();

        try {
            Namespace res = parser.parseArgs(args);

            /* parse args */
            String brokerList = res.getString("bootstrap.servers");
            String topic = res.getString("topic");
            String jksLocation = res.getString("jksLocation");
            String jksPassword = res.getString("jksPassword");


            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.RETRIES_CONFIG, 0);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

            // configure the following three settings for SSL Encryption
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, jksLocation);
            props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, jksPassword);

            // configure the following three settings for SSL Authentication
            props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, jksLocation);
            props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, jksPassword);
            props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, jksPassword);

            try (KafkaProducer<String, String> kProducer = new KafkaProducer<>(props)) {
                for (int i = 0; i < 10; i++) {
                    kProducer.send(new ProducerRecord<>(topic, Integer.toString(i), Integer.toString(i)), ((RecordMetadata metadata, Exception exception) -> {
                                if (exception != null) {
                                    exception.printStackTrace();
                                } else {
                                    System.out.format("Offset: %d, Partition: %d, Timestamp: %d%n ", metadata.offset(), metadata.partition(), metadata.timestamp());
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

        parser.addArgument("--topic").action(store())
                .required(true)
                .type(String.class)
                .metavar("TOPIC")
                .help("topic name");

        parser.addArgument("--jksLocation").action(store())
                .required(true)
                .type(String.class)
                .metavar("JKSLOCATION")
                .help("Jks certificate location");

        parser.addArgument("--jksPassword").action(store())
                .required(true)
                .type(String.class)
                .metavar("JKSPASSWORD")
                .help("Jks certificate password");

        return parser;
    }
}
