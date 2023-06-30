package cn.shh.test.kafka.common.deserializer;


import cn.shh.test.kafka.pojo.Company;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

public class CompanyDeserializer implements Deserializer<Company> {
    @Override
    public Company deserialize(String topic, byte[] data) {
        if (data == null){return null;}
        if (data.length < 8){
            throw new SerializationException("要反序列化的数据小于预期值");
        }

        ByteBuffer buffer = ByteBuffer.wrap(data);
        String name, address;
        int nameLen, addressLen;

        nameLen = buffer.getInt();
        byte[] nameBytes = new byte[nameLen];
        buffer.get(nameBytes);

        addressLen = buffer.getInt();
        byte[] addressBytes = new byte[addressLen];
        buffer.get(addressBytes);

        try {
            name = new String(nameBytes, "UTF-8");
            address = new String(addressBytes, "UTF-8");

        }catch (UnsupportedEncodingException e) {
            throw new SerializationException("反序列化出错！");
        }

        return new Company(name, address);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public Company deserialize(String topic, Headers headers, byte[] data) {
        return null;
    }

    @Override
    public void close() {}
}
