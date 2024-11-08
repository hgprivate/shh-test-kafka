package cn.shh.test.kafka.consumer.interceptor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class MyConsumerInterceptor implements ConsumerInterceptor<String, String> {
    private static final long EXPIRE_INTERVAL = 10 * 1000;

    /**
     * 消费者拿到消息前被调用
     * <p>
     * 在消费者拿到消息前，可以修改消息，也可以生成新消息。
     *
     * @param records records to be consumed by the client or records returned by the previous interceptors in the list.
     * @return
     */
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        log.info("MyConsumerInterceptor.onConsume");
        long now = System.currentTimeMillis();
        Map<TopicPartition, List<ConsumerRecord<String, String>>> newRecords = new HashMap<>();
        for (TopicPartition partition : records.partitions()){
            List<ConsumerRecord<String, String>> consumerRecords = records.records(partition);
            List<ConsumerRecord<String, String>> newConsumerRecords = new ArrayList<>();
            for (ConsumerRecord<String, String> record : consumerRecords){
                if (now - record.timestamp() < EXPIRE_INTERVAL){
                    newConsumerRecords.add(record);
                }
            }
            if (!newConsumerRecords.isEmpty()){
                newRecords.put(partition, newConsumerRecords);
            }
        }
        return new ConsumerRecords<>(newRecords);
    }

    /**
     * 提交偏移量时调用
     * @param offsets A map of offsets by partition with associated metadata
     */
    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        log.info("MyConsumerInterceptor.onCommit");
        offsets.forEach((tp, offset) -> {
            System.out.printf("tp：{}，offset：{}", tp, offset);
        });
    }

    /**
     * 拦截器关闭时被调用
     */
    @Override
    public void close() {
        log.info("MyConsumerInterceptor.close");
    }

    @Override
    public void configure(Map<String, ?> configs) {
        log.info("MyConsumerInterceptor.configure");
    }
}