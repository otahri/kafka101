package consumer;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

import static net.sourceforge.argparse4j.impl.Arguments.store;

public class SimpleConsumerSSL {
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
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group-kafka101");
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, jksLocation);
            props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, jksPassword);

            // configure the following three settings for SSL Authentication
            props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, jksLocation);
            props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, jksPassword);
            props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, jksPassword);

            try (KafkaConsumer<String, String> kConsumer = new KafkaConsumer<>(props)) {
                kConsumer.subscribe(Collections.singleton(topic));
                while (true) {
                    ConsumerRecords<String, String> cRecords = kConsumer.poll(100);
                    cRecords.forEach(record -> System.out.printf("Partition = %d, offset = %d, key = %s, value = %s\n", record.partition(), record.offset(), record.key(), record.value()));
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