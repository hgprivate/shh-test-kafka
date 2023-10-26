package cn.shh.test.kafka.controller;

import cn.shh.test.kafka.producer.ProducerAsync;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 作者：shh
 * 时间：2023/6/30
 * 版本：v1.0
 */
@RestController
@RequestMapping("/api/v1/async")
public class ProducerAsyncController {
    @Autowired
    private ProducerAsync producerAsync;

    // 1、发送消息，成功响应时调用回调
    @GetMapping("t01")
    public void test01(){
        producerAsync.test01();
    }

    // 2、异步发送消息到主题，发送被确认时调用回调
    @GetMapping("t02")
    private void test02(){
        producerAsync.test02();
    }

    // 3、异步发送消息到主题，发送被确认时调用回调
    // 描述：指定分区号
    @GetMapping("t03")
    private void test03() {
        producerAsync.test03();
    }

    // 4、异步发送消息到主题，发送被确认时调用回调
    // 描述：基于 自定义序列化器 发送 实体类
    @GetMapping("t04")
    private void test04()  {
        producerAsync.test04();
    }

    // 5、异步发送消息到主题，发送被确认时调用回调
    // 描述：使用自定义分区器
    @GetMapping("t05")
    private void test05()  {
        producerAsync.test05();
    }

    // 6、异步发送消息到主题，发送被确认时调用回调
    // 描述：使用自定义拦截器
    @GetMapping("t06")
    private void test06(){
        producerAsync.test06();
    }

    // 7、异步发送消息到主题，发送被确认时调用回调
    // 描述：使用事务来控制
    @GetMapping("t07")
    private void test07() {
        producerAsync.test07();
    }
}