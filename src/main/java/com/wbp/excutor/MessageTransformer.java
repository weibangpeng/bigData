package com.wbp.excutor;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.function.Function;
import com.alibaba.fastjson.JSON;

public class MessageTransformer implements Function<ConsumerRecord<String, Object>, Kemessge> {
    @Override
    public Kemessge call(ConsumerRecord<String, Object> v1) throws Exception {
        Object object = v1.value();
        JSONObject message = JSON.parseObject(object.toString());
        String key = v1.topic()+":"+v1.partition()+":"+v1.offset();
        return new Kemessge(key, message, v1.topic(),v1.partition(), v1.offset());
    }
}
