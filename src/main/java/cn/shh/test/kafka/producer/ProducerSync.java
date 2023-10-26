package cn.shh.test.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;

/**
 * 生产者 同步发送
 */
@Slf4j
@Service
public class ProducerSync {
    /*@Autowired
    private KafkaProducer kafkaProducer;*/

    @Autowired
    private KafkaTemplate kafkaTemplate;

    // 1、发送消息，成功响应时调用回调
    public void test01() {
        try {
            for (int i = 0; i < 5; i++) {
                kafkaTemplate.send(new ProducerRecord<String, String>("first", "豪哥最帅" + i)).get();
            }
        }catch (ExecutionException | InterruptedException e){
            e.printStackTrace();
        }
    }
}