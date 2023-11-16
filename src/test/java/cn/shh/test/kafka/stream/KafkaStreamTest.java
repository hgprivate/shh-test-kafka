package cn.shh.test.kafka.stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
@SpringBootTest
public class KafkaStreamTest {
    @Autowired
    private KafkaTemplate kafkaTemplate;

    /**
     * 发送消息
     */
    @Test
    public void sendMsg(){
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("test-stream-msg-in", "hello-kafkastream");
            kafkaTemplate.send(producerRecord);
        }
    }

    /**
     * 接收消息并统计分析，然后发出消息
     * @throws IOException
     */
    @Test
    public void streamInit() throws IOException {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamProcessor(streamsBuilder);

        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-kafka-stream-1");
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
        kafkaStreams.start();
        System.in.read();
    }
    private void streamProcessor(StreamsBuilder streamsBuilder) {
        // 这里的 test-stream-topic 表示从该topic中接收消息来分析统计
        KStream<String, String> kStream = streamsBuilder.stream("test-stream-msg-in");
        kStream.flatMapValues(new ValueMapper<String, Iterable<String>>() {
            @Override
            public Iterable<String> apply(String val) {
                return Arrays.asList(val.split(" "));
            }
        })      // 按val来分组聚合
                .groupBy((key, val) -> {
                    System.out.println("groupBy: " + key + " / " + val);
                    return val;
                })
                // 时间窗口
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10)))
                // 计算单词的数量
                .count()
                // 转为KStream
                .toStream()
                // 配置要返回的数据格式
                .map((key, val) -> {
                    System.out.println("map: " + key.key().toString() + " / " + val);
                    return new KeyValue<>(key.key().toString(), val.toString());
                })
                .to("test-stream-msg-out");
    }

    /**
     * 接收消息并消费
     */
    @Test
    public void consumeStreamMsg(){
        // 1、获取消费者
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-01");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        KafkaConsumer<String, String> kafkaConsumer= new KafkaConsumer(properties);

        // 2、订阅主题
        kafkaConsumer.subscribe(Collections.singletonList("test-stream-msg-out"));

        try {
            // 3、拉取消息并消费
            while (true){
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                consumerRecords.forEach(cr -> {
                    String key = cr.key();
                    String value = cr.value();
                    long offset = cr.offset();
                    int partition = cr.partition();
                    System.out.println("key: " + key + ", value: " + value + ", offset: " + offset + ", partition: " + partition);
                });
                // 4、异步提交偏移量
                kafkaConsumer.commitAsync();
            }
        } catch (Exception e) {
            System.out.println("error: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
}