package cn.shh.test.kafka.common.deserializer;

import cn.shh.test.kafka.pojo.Company;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class CompanyProtostuffDeserializer implements Deserializer<Company> {
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public Company deserialize(String topic, byte[] data) {
        if (data == null){return null;}
        Schema schema = RuntimeSchema.getSchema(Company.class);
        Company company = new Company();
        ProtostuffIOUtil.mergeFrom(data, company, schema);
        return company;
    }

    @Override
    public Company deserialize(String topic, Headers headers, byte[] data) {
        return null;
    }

    @Override
    public void close() {

    }
}
