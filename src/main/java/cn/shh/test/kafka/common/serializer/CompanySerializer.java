package cn.shh.test.kafka.common.serializer;

import cn.shh.test.kafka.pojo.Company;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

public class CompanySerializer implements Serializer<Company> {
    @Override
    public void configure(Map configs, boolean isKey) {}

    @Override
    public byte[] serialize(String topic, Company data) {
        if (data == null){
            return null;
        }
        byte[] name, address;
        try {
            if (data.getName() != null){
                name = data.getName().getBytes("UTF-8");
            }else {
                name = new byte[0];
            }

            if (data.getAddress() != null){
                address = data.getAddress().getBytes("UTF-8");
            }else {
                address = new byte[0];
            }

            ByteBuffer byteBuffer = ByteBuffer.allocate(4 + 4 + name.length + address.length);
            byteBuffer.putInt(name.length);
            byteBuffer.put(name);
            byteBuffer.putInt(address.length);
            byteBuffer.put(address);
            return byteBuffer.array();
        }catch (UnsupportedEncodingException e){
            e.printStackTrace();
        }
        return new byte[0];
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Company data) {
        return new byte[0];
    }

    @Override
    public void close() {}
}
