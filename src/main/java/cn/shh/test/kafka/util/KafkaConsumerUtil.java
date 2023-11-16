package cn.shh.test.kafka.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.Properties;

public class KafkaConsumerUtil {
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String KEY_DESERIALIZER_CLASS = StringDeserializer.class.getName();
    private static final String VALUE_DESERIALIZER_CLASS = StringDeserializer.class.getName();
    private static final String GROUP_ID = "consumer-group-01";
    private static KafkaConsumer kafkaConsumer;

    public static KafkaConsumer getKafkaConsumer(){
        if (kafkaConsumer == null){
            Properties properties = new Properties();
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,KEY_DESERIALIZER_CLASS);
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,VALUE_DESERIALIZER_CLASS);
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
            properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
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