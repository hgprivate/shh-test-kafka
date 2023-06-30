package cn.shh.test.kafka.producer.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 作者：shh
 * 时间：2023/6/30
 * 版本：v1.0
 */
public class MyPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        String msgVal = value.toString();
        int partition;
        if (msgVal.contains("豪哥")){
            partition = 0;
        }else{
            partition = 1;
        }
        return partition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
