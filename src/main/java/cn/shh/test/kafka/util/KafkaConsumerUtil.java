package cn.shh.test.kafka.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.Properties;

public class KafkaConsumerUtil {
    private static final String BOOTSTRAP_SERVERS = "kafka-node1:9092,kafka-node2:9092";
    private static final String KEY_DESERIALIZER_CLASS = StringDeserializer.class.getName();
    private static final String VALUE_DESERIALIZER_CLASS = StringDeserializer.class.getName();
    private static final String GROUP_ID = "consumer-group-01";
    private static KafkaConsumer kafkaConsumer;

    public static KafkaConsumer getKafkaConsumer(Map<String, Object> params){
        if (kafkaConsumer == null){
            Properties properties = new Properties();
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    params.getOrDefault(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KEY_DESERIALIZER_CLASS));
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    params.getOrDefault(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VALUE_DESERIALIZER_CLASS));
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
            kafkaConsumer = new KafkaConsumer(properties);
        }
        return kafkaConsumer;
    }

    public static void closeKafkaConsumer(KafkaConsumer kafkaConsumer){
        if (kafkaConsumer != null){
            kafkaConsumer.close();
        }
    }
}