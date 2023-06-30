package cn.shh.test.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.RoutingKafkaTemplate;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 作者：shh
 * 时间：2023/6/30
 * 版本：v1.0
 */
@Slf4j
@SpringBootTest
public class SendMessageTest {
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

    @Test
    public void isActive(){
        System.out.println("kafkaTemplate = " + kafkaTemplate);
    }

    @Test
    public void asyncSendMessageByKafkaTemplate(){
        ProducerRecord producerRecord = new ProducerRecord<>("first", "hello kafka.");
        CompletableFuture future = kafkaTemplate.send(producerRecord);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("send success.");
            }
            else {
                log.info("send error.");
            }
        });
    }

    @Test
    public void syncSendMessageByKafkaTemplate(){
        try {
            ProducerRecord producerRecord = new ProducerRecord<>("first", "hello kafka.");
            kafkaTemplate.send(producerRecord).get(10, TimeUnit.SECONDS);
            log.info("send success");
        } catch (InterruptedException e) {
            log.error("InterruptedException: {}", e);
        } catch (ExecutionException e) {
            log.error("ExecutionException: {}", e);
        } catch (TimeoutException e) {
            log.error("TimeoutException: {}", e);
        }


    }

    @Test
    public void sendMessageByRoutingKafkaTemplate(){
        routingKafkaTemplate.send("one", "thing1");
        routingKafkaTemplate.send("two", "thing2".getBytes());
    }
}
