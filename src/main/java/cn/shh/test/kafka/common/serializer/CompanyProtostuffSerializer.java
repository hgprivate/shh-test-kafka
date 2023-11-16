package cn.shh.test.kafka.common.serializer;

import cn.shh.test.kafka.pojo.Company;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class CompanyProtostuffSerializer implements Serializer<Company> {
    @Override
    public void configure(Map configs, boolean isKey) {}

    @Override
    public byte[] serialize(String topic, Company data) {
        if (data == null){return null;}
        Schema schema = RuntimeSchema.getSchema(data.getClass());
        LinkedBuffer linkedBuffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
        byte[] protostuff = null;
        try {
            protostuff = ProtostuffIOUtil.toByteArray(data, schema, linkedBuffer);
        }catch (Exception e){
            throw new IllegalStateException(e.getMessage(), e);
        }finally {
            linkedBuffer.clear();
        }
        return protostuff;
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Company data) {
        return new byte[0];
    }

    @Override
    public void close() {}
}