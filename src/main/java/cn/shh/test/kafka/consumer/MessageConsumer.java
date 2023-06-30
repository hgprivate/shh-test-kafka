package cn.shh.test.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * 作者：shh
 * 时间：2023/6/30
 * 版本：v1.0
 */
@Slf4j
@Component
public class MessageConsumer {
    @KafkaListener(topics = {"first"})
    public void kafkaListener(Object message){
        log.info("message: {}", message.toString());
    }

    @KafkaListener(topics = {"one", "two"})
    public void kafkaListener2(Object message){
        log.info("message: {}", message);
    }
}
