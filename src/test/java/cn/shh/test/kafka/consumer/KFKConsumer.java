package cn.shh.test.kafka.consumer;

import cn.shh.test.kafka.kafka3.util.KafkaConsumerUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 消费者 客户端
 *
 * - Kafka 基于拉模式 来消费消息。
 * - KafkaConsumer线程非安全。
 *
 * - 控制和关闭消费 通过 pause() 和 resume()方法实现。
 * - 指定位移消费可调用seek()方法实现。
 */
public class KFKConsumer {
    private static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static void main(String[] args) {
        // 1、配置参数，拿到消费者实例
        KafkaConsumer consumer = getKafkaConsumer();

        // 2、订阅主题
        asignedTopic(consumer);

        // 3、拉取消息并消费
        consumerMsg(consumer);

        // 4、关闭消费者实例
        consumer.close();
    }

    /**
     * 1、配置参数并创建消费者实例
     */
    private static KafkaConsumer getKafkaConsumer() {
        Map<String, Object> map = new HashMap<>();
        // 使用 自定义反序列化器
        //map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CompanyDeserializer.class.getName());
        // 使用 通用Protostuff序列化工具
        //map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CompanyProtostuffDeserializer.class.getName());
        // 使用 自定义拦截器
        //map.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, ConsumerInterceptorTTL.class.getName());
        KafkaConsumer consumer = KafkaConsumerUtil.getKafkaConsumer(map);
        return consumer;
    }

    /**
     * 2、订阅主题
     *
     * 一个消费者可以订阅一个或多个主题。订阅方式有两种：
     *   - 集合方式：subscribe(Collection)
     *     例如：consumer.subscribe(Arrays.asList("first"));
     *   - 正则表达式方式：subscribe(Pattern)
     *     例如：consumer.subscribe(Pattern.compile("first*"));
     *
     * 要想订阅主题中的特定分区可用assign()方法来代替。
     *
     * 取消订阅某个主题可以使用consumer.unsubscribe()方法。如果subscribe方法和
     * assign方法入参集合为空，那么其作用相当于调用unsubscribe()方法。
     *
     * 集合订阅方式、正则订阅方式、指定分区订阅方式 分别代表三种订阅状态：
     *   - AUTO_TOPICS
     *   - AUTO_PATTERN
     *   - USER_ASSIGNED
     * 如果没有订阅，那么订阅状态为 NONE。该三种状态互斥，一个消费者只能使用其中的
     * 一种方式，否则会抛异常。
     *
     * 消费者自动再均衡
     *      - subscribe()方法具备 消费者自动再均衡功能，而assign()方法不具备该功能。
     *
     * 注意事项：
     *      - 如果前后两次订阅了不同的主题，那么以最后一次订阅为准。
     */
    private static void asignedTopic(KafkaConsumer consumer) {
        // 集合方式 订阅 主题
        //consumer.subscribe(Arrays.asList("first"));
        // 正则表达式方式 订阅 主题
        //consumer.subscribe(Pattern.compile("first*"));
        // 订阅 指定主题中的指定分区
        consumer.assign(Arrays.asList(new TopicPartition("first", 0)));

        // 订阅 主题的全部分区
        /*List<TopicPartition> partitions = null;
        List<PartitionInfo> partitionsFor = consumer.partitionsFor("first");
        if (partitionsFor != null){
            partitions = new ArrayList<>();
            for (PartitionInfo info : partitionsFor){
                partitions.add(new TopicPartition(info.topic(), info.partition()));
            }
        }
        consumer.assign(partitions);*/

        // 使用 再均衡监听器
        /*consumer.subscribe(Arrays.asList("first"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

            }
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

            }
        });*/
    }

    /**
     * 3、拉取消息、消费消息、提交消费位移
     *
     * 拉取消息
     *
     * 消费消息
     *
     * 提交消费位移
     *      - 自动提交功能 依赖 如下参数：
     *          - 参数 enable.auto.commit 表示 是否开启自动位移提交。默认开启，即默认自动提交。
     *          - 参数 auto.commit.interval.ms 表示 多次自动提交的时间间隔。默认5秒。
     *      - 手动提交功能 分两种：
     *          - 同步提交：commitSync()方法
     *          - 异步提交：commitAsync()方法
     */
    private static void consumerMsg(KafkaConsumer consumer) {
        // 设置从 指定offset位置开始消费
        /*Set<TopicPartition> assignment = consumer.assignment();
        for (TopicPartition partition : assignment) {
            consumer.seek(partition,300);
        }*/

        // 设置从 指定时间 开始消费
        /*Map<TopicPartition, Long> partitionLongMap = new HashMap<>();
        for (TopicPartition partition : assignment) {
            partitionLongMap.put(partition, System.currentTimeMillis() - 1 * 24 * 3600 * 1000);
        }
        Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = consumer.offsetsForTimes(partitionLongMap);
        for (TopicPartition partition : assignment) {
            OffsetAndTimestamp andTimestamp = offsetsForTimes.get(partition);
            consumer.seek(partition, andTimestamp.offset());
        }*/

        // 消费消息
        long lastConsumedOffset = -1;
        while (isRunning.get()){
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord record : consumerRecords){
                System.out.println("record：" + record);
            }


            // 同步提交消费位移
            consumer.commitSync();

            // 按分区粒度同步提交消费位移
            /*consumer.commitSync(Collections.singletonMap(new TopicPartition("first", 0),
                    new OffsetAndMetadata(lastConsumedOffset + 1)));*/

            // 异步提交完成后调用回调方法
            /*consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    if (exception == null){
                        log.info("offsets：{}", offsets);
                    }else {
                        log.error("异步提交位移 失败！");
                    }
                }
            });*/
        }
    }
}
