package com.wbp.spark;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.streaming.kafka010.ConsumerStrategy;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class MycomsumerPolicy extends ConsumerStrategy <String,Object>{

    Map<String, Object> kafkaParams;
    List<String> topics;
    int startType;
    Map<TopicPartition, Long> offset;

    public MycomsumerPolicy(Map<String, Object> kafkaParams, List<String> topics, int startType, Map<TopicPartition, Long> offset) {
        this.kafkaParams = kafkaParams;
        this.topics = topics;
        this.startType = startType;
        this.offset = offset;
    }

    @Override
    public Map<String, Object> executorKafkaParams() {
        return kafkaParams;
    }

    @Override
    public Consumer<String, Object> onStart(Map<TopicPartition, Long> currentOffset) {
        KafkaConsumer<String, Object> consumer = new KafkaConsumer<String, Object>(kafkaParams);
        consumer.subscribe(topics);

        Map<TopicPartition, Long> toseek = currentOffset.isEmpty()? offset:currentOffset;
        consumer.poll(0);
        Set<TopicPartition> assignment = consumer.assignment();
        if(0==startType){
            consumer.seekToBeginning(assignment);
        }
        if(2==startType){
            consumer.seekToEnd(assignment);
        }
        if(1==startType){
            toseek.forEach(consumer::seek);
        }

        return consumer;
    }
}
