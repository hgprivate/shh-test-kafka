package cn.shh.test.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * 作者：shh
 * 时间：2023/6/30
 * 版本：v1.0
 *
 * kafka topic config
 */
@Configuration
public class KafkaTopicConfig {
    @Bean
    public NewTopic firstTopic(){
        return TopicBuilder.name("first").build();
    }

    /*@Bean
    public KafkaAdmin.NewTopics topics(){
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name("defaultBoth")
                        .build(),
                TopicBuilder.name("defaultPart")
                        .replicas(1)
                        .build(),
                TopicBuilder.name("defaultRepl")
                        .partitions(3)
                        .build());
    }*/

    @Bean
    public KafkaAdmin.NewTopics topics(){
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name("one")
                        .build(),
                TopicBuilder.name("two")
                        .build());
    }
}
