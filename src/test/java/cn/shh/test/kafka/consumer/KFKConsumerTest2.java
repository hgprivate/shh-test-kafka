package cn.shh.test.kafka.consumer;

import cn.shh.test.kafka.common.deserializer.CompanyDeserializer;
import cn.shh.test.kafka.util.KafkaConsumerUtil;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 消息消费者 问题重现及应对
 */
public class KFKConsumerTest2 {
    private static final AtomicBoolean isRunning = new AtomicBoolean(true);

    /**
     * 位移提交失败后会重试，重试会增加代码难度，不重试会增加重复消费概率。
     * <p>
     * 如果消费者正常退出或发生再均衡，那么可在退出或再均衡前使用同步提交来做保障。
     */
    private static void test01() {
        // 1、配置参数并创建消费者实例
        Map<String, Object> map = new HashMap<>();
        map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CompanyDeserializer.class.getName());
        map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "");
        KafkaConsumer consumer = KafkaConsumerUtil.getKafkaConsumer();

        // 2、订阅主题
        consumer.subscribe(Arrays.asList("first"));       // 集合方式 订阅 主题
        //consumer.subscribe(Pattern.compile("first*"));  // 正则表达式方式 订阅 主题

        // 3、拉取消息并消费
        long lastConsumedOffset = -1;
        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                if (consumerRecords.isEmpty()) { // 慎用，这里仅仅用作演示。
                    break;
                }
                List<ConsumerRecord<String, String>> records = consumerRecords.records(new TopicPartition("first", 0));
                lastConsumedOffset = records.get(records.size() - 1).offset();

                // 4、提交消费位移
                consumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        if (exception == null) {
                            //log.info("offsets：{}", offsets);
                        } else {
                            //log.error("异步提交位移 失败！");
                        }
                    }
                });

                /*log.info("comsumed offset：{}，commited offset：{}，next record offset：{}",
                        lastConsumedOffset, consumer.committed(new TopicPartition("first", 0)).offset(),
                        consumer.position(new TopicPartition("first", 0)));*/
            }
        } finally {
            // 最终通过 同步提交 来做最后的保障。
            try {
                consumer.commitSync();
            } finally {
                consumer.close();
            }
        }
    }
}