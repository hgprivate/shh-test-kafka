package cn.shh.test.kafka.producer.partition;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 作者：shh
 * 时间：2023/6/30
 * 版本：v1.0
 */
@Slf4j
public class MyPartitioner implements Partitioner {
    /**
     * 为记录计算分区
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        log.info("MyPartitioner.partition(......)");
        String msgVal = value.toString();
        int partition;
        if (msgVal.contains("豪哥")){
            partition = 0;
        }else{
            partition = 1;
        }
        return partition;
    }

    /**
     * 分区关闭时被调用
     */
    @Override
    public void close() {
        log.info("MyPartitioner.close()");
    }

    @Override
    public void configure(Map<String, ?> configs) {
        log.info("MyPartitioner.configure(.)");
    }
}