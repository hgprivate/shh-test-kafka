package cn.shh.test.kafka.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerUtil {
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String KEY_SERIALIZER = StringSerializer.class.getName();
    private static final String VALUE_SERIALIZER = StringSerializer.class.getName();
    private static final String ACKS = "all";
    private static final int RETRIES = 10;
    private static KafkaProducer kafkaProducer;

    public static KafkaProducer getKafkaProducer(){
        if (kafkaProducer == null){
            Properties properties = new Properties();
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER);
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VALUE_SERIALIZER);
            properties.put(ProducerConfig.RETRIES_CONFIG, RETRIES);
            //properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "");
            //properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "");
            properties.put(ProducerConfig.ACKS_CONFIG, ACKS);
            kafkaProducer = new KafkaProducer(properties);
        }
        return kafkaProducer;
    }

    public static void closeKafkaProducer(KafkaProducer kafkaProducer){
        if (kafkaProducer != null){
            kafkaProducer.close();
        }
    }
}