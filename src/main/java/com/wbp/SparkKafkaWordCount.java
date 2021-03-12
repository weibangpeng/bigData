

package com.wbp;

import com.wbp.annotation.EnableMasterSpringBoot;
import com.wbp.excutor.Kemessge;
import com.wbp.excutor.MessageTransformer;
import com.wbp.excutor.StreamContext;
import com.wbp.excutor.StreamExcutor;
import com.wbp.spark.MycomsumerPolicy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.*;


@EnableMasterSpringBoot
public  class SparkKafkaWordCount {

    static ConfigurableApplicationContext  applicationContext;
    Logger logger = LoggerFactory.getLogger(SparkKafkaWordCount.class);
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
        SpringApplication application =new SpringApplication(SparkKafkaWordCount.class);
        application.setWebEnvironment(false);

        application.setRegisterShutdownHook(false);
        applicationContext =  application.run();
        SparkKafkaWordCount directKafkaWordCount = applicationContext.getBean(SparkKafkaWordCount.class);
        directKafkaWordCount.start(args,new StreamContext(applicationContext));
    }

    void start(String[] args,StreamContext context) {
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

        JavaDStream<Kemessge> Kemessges= dStream.map(new MessageTransformer());
        Kemessges.foreachRDD((VoidFunction2<JavaRDD<Kemessge>, Time>) (rdd, time) -> {
            OffsetRange[] offsetRanges = offsetRangMap.remove(time);
            for (int i = 0;i<offsetRanges.length;i++){
                OffsetRange o = offsetRanges[i];
                logger.info(
                        o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
            }
            rdd.foreachPartition(new StreamExcutor(context));
            canCommitOffsets.commitAsync(offsetRanges,(offsetcb,exception)->{
                logger.info("commitAsync :"+offsetcb);
            });

        });

        // Start the computation
        jssc.start();
        try {
            jssc.awaitTermination();
        }catch (Exception e){
            logger.info("",e);
        }

    }

}