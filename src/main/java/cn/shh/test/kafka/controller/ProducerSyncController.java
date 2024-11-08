package cn.shh.test.kafka.controller;

import cn.shh.test.kafka.producer.ProducerSync;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/sync")
public class ProducerSyncController {
    @Autowired
    private ProducerSync producerSync;

    // 1、发送消息，成功响应时调用回调
    @GetMapping("t01")
    public void test01(){
        producerSync.test01();
    }
}