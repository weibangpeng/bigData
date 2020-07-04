/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wbp;

import java.util.*;
import java.util.regex.Pattern;

import org.apache.kafka.common.TopicPartition;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.kafka010.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.context.ConfigurableApplicationContext;
import scala.Tuple2;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.Durations;


/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: JavaDirectKafkaWordCount <brokers> <groupId> <topics>
 *   <brokers> is a list of one or more Kafka brokers
 *   <groupId> is a consumer group name to consume from topics
 *   <topics> is a list of one or more kafka topics to consume from
 *
 * Example:
 *    $ bin/run-example streaming.JavaDirectKafkaWordCount broker1-host:port,broker2-host:port \
 *      consumer-group topic1,topic2
 */

@EnableWorkerSpringBoot
public final class JavaDirectKafkaWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    private Map<Time,OffsetRange[]> offsetRangMap = new HashMap<>();
    public static void main(String[] args)  {
        if (args.length < 4) {
            System.err.println("Usage: JavaDirectKafkaWordCount <brokers> <groupId> <topics>\n" +
                    "  <brokers> is a list of one or more Kafka brokers\n" +
                    "  <groupId> is a consumer group name to consume from topics\n" +
                    "  <topics> is a consumer group name to consume from topics\n" +
                    "  <startType> is a int ,0 frombegining \n\n");
            System.exit(1);
        }
        SpringApplication application =new SpringApplication(JavaDirectKafkaWordCount.class);
        application.setWebApplicationType(WebApplicationType.NONE);
        ConfigurableApplicationContext applicationContext =  application.run();
        JavaDirectKafkaWordCount directKafkaWordCount = applicationContext.getBean(JavaDirectKafkaWordCount.class);
        directKafkaWordCount.start(args);
    }

    void start(String[] args) {
        String brokers = args[0];
        String groupId = args[1];
        String topics = args[2];
        int startType = Integer.parseInt(args[3]);

        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        List<String> topicsList = new ArrayList<String>(Arrays.asList(topics.split(",")));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        Map<TopicPartition, Long> offset = new HashMap<>();
        MycomsumerPolicy mycomsumerPolicy=new MycomsumerPolicy(kafkaParams,topicsList,startType,offset);
        // Create direct kafka stream with brokers and topics
        JavaDStream<ConsumerRecord<String, Object>> dStream = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                mycomsumerPolicy);

        CanCommitOffsets canCommitOffsets = (CanCommitOffsets) dStream.dstream();
        //获取offset
        dStream = dStream.transform((Function2<JavaRDD<ConsumerRecord<String, Object>>, Time,JavaRDD<ConsumerRecord<String, Object>>>) (rdd, time)->{
            offsetRangMap.put(time,((HasOffsetRanges) rdd.rdd()).offsetRanges());
            return rdd;
        });
        /*messages.foreachRDD(rdd ->{
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            ((CanCommitOffsets) messages.inputDStream()).commitAsync(offsetRanges,(offsetcb,exception)->{
                System.out.println("commitAsync :"+offsetcb);
            });
        });*/
        JavaDStream<Kemessge> Kemessges= dStream.map(new MessageTransformer());
        Kemessges.foreachRDD((VoidFunction2<JavaRDD<Kemessge>, Time>) (rdd, time) -> {
            OffsetRange[] offsetRanges = offsetRangMap.remove(time);
            for (int i = 0;i<offsetRanges.length;i++){
                OffsetRange o = offsetRanges[i];
                System.out.println(
                        o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
            }

            rdd.foreachPartition(new StreamExcutor());
            canCommitOffsets.commitAsync(offsetRanges,(offsetcb,exception)->{
                System.out.println("commitAsync :"+offsetcb);
            });

        });

        // Start the computation
        jssc.start();
        try {
            jssc.awaitTermination();
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}