package cn.shh.test.kafka;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootTest
class KafkaBaseTest {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    public void isActive() {
        System.out.println("kafkaTemplate = " + kafkaTemplate);
    }

    @Test
    public void sendMessage(){
        kafkaTemplate.send("first", "hello kafka");
    }
}
