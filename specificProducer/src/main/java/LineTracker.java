import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;
import java.util.stream.Collectors;

public class LineTracker {
    private KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(getKafkaConfig());

    private Properties getKafkaConfig (){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        //props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        return props;
    }

    int getLineTracker(String fileName){
        List<Integer> lastMessages = new ArrayList<>();
        List<PartitionInfo> lPartitions = kafkaConsumer.partitionsFor("eo-topic-1p");
        List<TopicPartition> topicPartitionList = lPartitions.stream().map(partitionInfo -> new TopicPartition(partitionInfo.topic(),partitionInfo.partition())).collect(Collectors.toList());
        kafkaConsumer.assign(topicPartitionList);
        Map<TopicPartition, Long> lastOffsets= kafkaConsumer.endOffsets(topicPartitionList).entrySet().stream()
                .filter(x -> x.getValue() >= 1).collect(Collectors.toMap(Map.Entry::getKey, p -> p.getValue() -1));

        lastOffsets.forEach((x, y) -> System.out.println(x.topic() + " partition: " + x.partition() + " offset: " + y));


        if (!lastOffsets.isEmpty()){
            while (lastMessages.size()< lastOffsets.size()) {
                lastOffsets.forEach((topicPartition, value) -> kafkaConsumer.seek(topicPartition, value));
                kafkaConsumer.poll(1000).iterator().forEachRemaining(cr ->
                        lastMessages.add(Integer.parseInt(cr.key().replace(fileName + "_", ""))));
            }
            Integer returnValue = lastMessages.stream().mapToInt(v -> v).max().orElseThrow(NoSuchElementException::new);
            System.out.println(returnValue);
            return returnValue;
        } else
            return 0;
    }

}
