package cn.shh.test.kafka.controller;

import cn.shh.test.kafka.records.MessageRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 作者：shh
 * 时间：2023/6/30
 * 版本：v1.0
 */
@RestController
@RequestMapping("/api/v1/message")
public class KafkaController {
    @Autowired
    private KafkaTemplate kafkaTemplate;

    @PostMapping
    public void publish(@RequestBody MessageRequest messageRequest){
        kafkaTemplate.send("first", messageRequest.message());
    }
}