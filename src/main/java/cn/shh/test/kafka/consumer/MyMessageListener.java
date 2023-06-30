package cn.shh.test.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * 作者：shh
 * 时间：2023/6/30
 * 版本：v1.0
 */
@Slf4j
@Component
public class MyMessageListener implements MessageListener {
    @Override
    public void onMessage(Object data) {
        log.info("data: {}", data);
    }

    @Override
    public void onMessage(Object data, Acknowledgment acknowledgment) {
        log.info("data: {}, acknowledgment: {}", data, acknowledgment);
    }

    @Override
    public void onMessage(Object data, Consumer consumer) {
        log.info("data: {}, consumer: {}", data, consumer);
    }

    @Override
    public void onMessage(Object data, Acknowledgment acknowledgment, Consumer consumer) {
        log.info("data: {}, acknowledgment: {}, consumer: {}", data, acknowledgment, consumer);
    }
}
