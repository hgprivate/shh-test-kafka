package cn.shh.test.kafka.consumer;

import cn.shh.test.kafka.util.KafkaConsumerUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * KafkaConsumer多线程实现
 *
 * KafkaConsumer非线程安全。acquire()方法可用来检测当前是否只有一个线程在操作，其对应的是
 * release()方法。
 * <p>
 * 多线程实现方式有多种，例如：<ol>
 *   <li> 线程封闭：为每个线程实例化一个KafkaConsumer对象。
 *   <li> 多个线程同时消费同一分区。通过assign()、seek()方法实现。
 *   <li> 基于多线程实现消息处理
 */
@Slf4j
public class MultiThreadKFKConsumer {

    public static void main(String[] args) {
        MultiThreadKFKConsumer consumer = new MultiThreadKFKConsumer();

        consumer.test01();
//        consumer.test02();
    }

    /**
     * 多线程实现：线程封闭
     */
    private void test01() {
        for (int i = 0; i < 4; i++) {
            new KafkaConsumerThread("first").start();
        }
    }
    private class KafkaConsumerThread extends Thread{
        private KafkaConsumer<String, String> kafkaConsumer;

        public KafkaConsumerThread(String topic){
            this.kafkaConsumer = KafkaConsumerUtil.getKafkaConsumer(null);
            this.kafkaConsumer.subscribe(Arrays.asList(topic));
        }
        @Override
        public void run() {
            try {
                while(true){
                    ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String > consumerRecord : consumerRecords){
                        // 处理消息
                    }
                }
            }catch (Exception e){

            }finally {
                kafkaConsumer.close();
            }
        }
    }


    /**
     * 多线程实现：多线程处理消息
     */
    private void test02() {
        for (int i = 0; i < 4; i++) {
            new KafkaConsumerThread2("first",3).start();
        }
    }
    private class KafkaConsumerThread2 extends Thread{
        private KafkaConsumer<String, String> kafkaConsumer;
        private ExecutorService executorService;
        private int threadNumber;
        public KafkaConsumerThread2(String topic, int threadNumber){
            kafkaConsumer = KafkaConsumerUtil.getKafkaConsumer(null);
            kafkaConsumer.subscribe(Collections.singletonList(topic));
            this.threadNumber = threadNumber;
            executorService = new ThreadPoolExecutor(threadNumber, threadNumber, 0L,
                    TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1000),
                    new ThreadPoolExecutor.CallerRunsPolicy());
        }
        @Override
        public void run() {
            try {
                while (true){
                    ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
                    if (!consumerRecords.isEmpty()){
                        executorService.submit(new RecordsHandler(consumerRecords));
                    }
                }
            }catch (Exception e){
                e.printStackTrace();
            }finally {
                kafkaConsumer.close();
            }
        }
    }
    private class RecordsHandler extends Thread{
        public final ConsumerRecords<String, String> consumerRecords;
        public RecordsHandler(ConsumerRecords<String, String> consumerRecords){
            this.consumerRecords = consumerRecords;
        }
        @Override
        public void run() {
            for (TopicPartition tp : consumerRecords.partitions()){
                List<ConsumerRecord<String, String>> tpRecords = consumerRecords.records(tp);
                long lastConsumedOffset = tpRecords.get(tpRecords.size() - 1).offset();
                log.info("lastConsumedOffset：{}", lastConsumedOffset);
            }
        }
    }
}