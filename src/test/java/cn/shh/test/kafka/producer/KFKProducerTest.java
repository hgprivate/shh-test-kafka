package cn.shh.test.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.RoutingKafkaTemplate;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 消息生产者
 */
@Slf4j
@SpringBootTest
public class KFKProducerTest {
    @Autowired
    private KafkaTemplate kafkaTemplate;
    @Autowired
    private RoutingKafkaTemplate routingKafkaTemplate;
//
//    @Autowired
//    private DefaultKafkaProducerFactory defaultKafkaProducerFactory;
//
//    @Autowired
//    private ReplyingKafkaTemplate replyingKafkaTemplate;

    /**
     * 测试 KafkaTemplate 实例是否创建并注入成功
     */
    @Test
    public void isActive() {
        System.out.println("kafkaTemplate：" + kafkaTemplate);
        System.out.println("routingKafkaTemplate：" + routingKafkaTemplate);
    }

    /**
     * 通过 KafkaTemplate 异步发送消息
     */
    @Test
    public void asyncSendMessageByKafkaTemplate() {
        ProducerRecord producerRecord = new ProducerRecord<>("first", "hello kafka.");
        CompletableFuture future = kafkaTemplate.send(producerRecord);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("send success.");
            } else {
                log.info("send error.");
            }
        });
    }

    /**
     * 通过 KafkaTemplate 同步发送消息
     */
    @Test
    public void syncSendMessageByKafkaTemplate() {
        try {
            ProducerRecord producerRecord = new ProducerRecord<>("first", "hello kafka.");
            Object result = kafkaTemplate.send(producerRecord).get(10, TimeUnit.SECONDS);
            System.out.println("result = " + result);
            log.info("send success");
        } catch (InterruptedException e) {
            log.error("InterruptedException: {}", e);
        } catch (ExecutionException e) {
            log.error("ExecutionException: {}", e);
        } catch (TimeoutException e) {
            log.error("TimeoutException: {}", e);
        }
    }

    /**
     * 通过 RoutingKafkaTemplate 发送消息
     * <p>
     * 向指定主题发送消息
     * </p>
     */
    @Test
    public void sendMessageByRoutingKafkaTemplate() {
        routingKafkaTemplate.send("one", "thing1");
        routingKafkaTemplate.send("two", "thing2".getBytes());
    }
}