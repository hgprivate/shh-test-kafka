package cn.shh.test.kafka.producer.interceptor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 作者：shh
 * 时间：2023/6/30
 * 版本：v1.0
 */
@Slf4j
public class MyProducerInterceptor implements ProducerInterceptor<String, String> {
    private volatile long sendSuccess = 0;
    private volatile long sendFailure = 0;

    /**
     * 发送消息前 被调用
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        String newVal = "prefix - " + producerRecord.value();
        return new ProducerRecord(producerRecord.topic(), producerRecord.partition(), newVal,
                producerRecord.headers());
    }

    /**
     * 当发送到服务器的记录已被确认，或记录在发送到服务器之前失败时调用此方法。
     */
    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception exception) {
        if (exception == null){
            sendSuccess++;
        }else {
            sendFailure++;
        }
    }

    /**
     * 拦截器关闭时 被调用
     */
    @Override
    public void close() {
        double successRatio = (double) sendSuccess / (sendSuccess + sendFailure);
        log.info("发送成功率：" + String.format("%f", successRatio * 100) + "%");
    }

    @Override
    public void configure(Map<String, ?> map) {
    }
}