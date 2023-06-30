package cn.shh.test.kafka.producer;

import cn.shh.test.kafka.pojo.Company;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * 生产者 异步发送
 */
@Slf4j
@Service
public class ProducerAsync {
    @Autowired
    public KafkaProducer kafkaProducer;

    @Autowired
    private KafkaTemplate kafkaTemplate;

    // 1、发送消息，成功响应时调用回调
    public void test01(){
        for (int i = 0; i < 5; i++) {
            ProducerRecord<Object, String> producerRecord = new ProducerRecord<>("first", ("豪哥最帅" + i));
            kafkaTemplate.send(producerRecord).whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("send success.");
                }
                else {
                    log.info("send error.");
                }
            });
        }
    }

    // 2、异步发送消息到主题，发送被确认时调用回调
    public void test02(){
        for (int i = 0; i < 5; i++) {
            ProducerRecord<Object, String> producerRecord = new ProducerRecord<>("first", ("豪哥最帅" + i));
            kafkaTemplate.send(producerRecord).whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("send success.");
                }
                else {
                    log.info("send error.");
                }
            });
        }
    }

    // 3、异步发送消息到主题，发送被确认时调用回调
    // 描述：指定分区号
    public void test03() {
        try {
            for (int i = 0; i < 5; i++) {
                // 构造消息，指定消息要发送至的分区
                ProducerRecord record = new ProducerRecord<>("first", 1, "a", "豪哥最帅" + i);
                Future<RecordMetadata> future = kafkaTemplate.send(record);
                RecordMetadata recordMetadata = future.get();
                log.info("topic:{}, partition:{}, offset:{}", recordMetadata.topic(),
                        recordMetadata.partition(), recordMetadata.offset());
            }
        }catch (ExecutionException | InterruptedException e){
            e.printStackTrace();
        }
    }

    // 4、异步发送消息到主题，发送被确认时调用回调
    // 描述：基于 自定义序列化器 发送 实体类
    public void test04()  {
        try {
            for (int i = 0; i < 5; i++) {
                // 构造消息，指定消息要发送至的分区
                Company company = Company.builder().name("豪哥科技").address("China").build();
                ProducerRecord<String, Company> record = new ProducerRecord<>("first", company);
                Future<RecordMetadata> future = kafkaTemplate.send(record);
                RecordMetadata recordMetadata = future.get();

                log.info("topic:{}, partition:{}, offset:{}", recordMetadata.topic(),
                        recordMetadata.partition(), recordMetadata.offset());
            }
        }catch (ExecutionException | InterruptedException e){
            e.printStackTrace();
        }
    }

    // 5、异步发送消息到主题，发送被确认时调用回调
    // 描述：使用自定义分区器
    public void test05()  {
        try {
            for (int i = 0; i < 5; i++) {
                // 构造消息，指定消息要发送至的分区
                ProducerRecord<String, String> record = new ProducerRecord<>("first", "使用自定义分区器");
                Future<RecordMetadata> future = kafkaTemplate.send(record);
                RecordMetadata recordMetadata = future.get();

                log.info("topic:{}, partition:{}, offset:{}", recordMetadata.topic(),
                        recordMetadata.partition(), recordMetadata.offset());
            }
        }catch (ExecutionException | InterruptedException e){
            e.printStackTrace();
        }
    }

    // 6、异步发送消息到主题，发送被确认时调用回调
    // 描述：使用自定义拦截器
    public void test06(){
        try {
            for (int i = 0; i < 5; i++) {
                // 构造消息，指定消息要发送至的分区
                ProducerRecord<String, String> record = new ProducerRecord<>("first", "使用自定义拦截器");
                Future<RecordMetadata> future = kafkaTemplate.send(record);
                RecordMetadata recordMetadata = future.get();

                log.info("topic:{}, partition:{}, offset:{}", recordMetadata.topic(),
                        recordMetadata.partition(), recordMetadata.offset());
            }
        }catch (ExecutionException | InterruptedException e){
            e.printStackTrace();
        }
    }

    // 7、异步发送消息到主题，发送被确认时调用回调
    // 描述：使用事务来控制
    public void test07() {
        /*Map<String, Object> map = new HashMap<>();
        map.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction_id_01");
        KafkaProducer producer = KafkaProducerUtil.getKafkaProducer(map);*/

        // 初始化事务
        //kafkaTemplate.initTransactions();
        // 开启事务
        //kafkaTemplate.beginTransaction();

        try {
            for (int i = 0; i < 5; i++) {
                // 构造消息，指定消息要发送至的分区
                ProducerRecord<String, String> record = new ProducerRecord<>("first", "使用自定义拦截器");
                Future<RecordMetadata> future = kafkaTemplate.send(record);
                RecordMetadata recordMetadata = future.get();

                log.info("topic:{}, partition:{}, offset:{}", recordMetadata.topic(),
                        recordMetadata.partition(), recordMetadata.offset());
            }

            // 提交事务
            //kafkaTemplate.commitTransaction();
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
