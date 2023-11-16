package cn.shh.test.kafka.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * kafka stream 流配置
 */

@Configuration
@EnableKafkaStreams
public class KafkaStreamConfig {
    @Bean
    public KafkaStreamsConfiguration defaultKafkaStreamsConfig(KafkaProperties kafkaProperties){
        Map<String, Object> params = new HashMap<>();
        params.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        params.put(StreamsConfig.APPLICATION_ID_CONFIG, kafkaProperties.getStreams().getApplicationId());
        params.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        params.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return new KafkaStreamsConfiguration(params);
    }

    @Bean
    public KStream<String, String> streamProcessor(StreamsBuilder streamsBuilder) {
        KStream<String, String> kStream = streamsBuilder.stream("test-stream-topic");
        kStream.flatMapValues(new ValueMapper<String, Iterable<String>>() {
                    @Override
                    public Iterable<String> apply(String val) {
                        return Arrays.asList(val.split(" "));
                    }
                }).groupBy((key, val) -> val)
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10)))
                .count()
                .toStream()
                .map((key, val) -> {
                    return new KeyValue<>(key.key().toString(), val.toString());
                })
                .to("test-stream-msg-out");
        return kStream;
    }
}