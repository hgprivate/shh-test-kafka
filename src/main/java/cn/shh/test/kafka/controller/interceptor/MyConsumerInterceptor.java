package cn.shh.test.kafka.controller.interceptor;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 作者：shh
 * 时间：2023/6/30
 * 版本：v1.0
 */
public class MyConsumerInterceptor implements ConsumerInterceptor<String, String> {
    private static final long EXPIRE_INTERVAL = 10 * 1000;

    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
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

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        offsets.forEach((tp, offset) -> {
            System.out.printf("tp：{}，offset：{}", tp, offset);
        });
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
