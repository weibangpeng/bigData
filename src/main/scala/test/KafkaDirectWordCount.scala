package test

import java.io

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import org.apache.hadoop.hbase.client.Result
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.clients.KafkaClient
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Assign
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}
import test.HbaseConn
import scala.util.control.Breaks._
import scala.collection.mutable.ListBuffer
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties

/*
description:sparkSteaming scala版本
reference:http://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html#storing-offsets
date:2020.05
 */


object KafkaDirectWordCount {

/*  def kafkaConsumer(){
    val BOOTSTRAP_SERVERS="admin.ambari.com:6667"
    val GROUPID="meteorological1"
    val props = new Properties()

    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUPID)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[Nothing].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[Nothing].getName)
    val consumer = new KafkaConsumer[_, _](props)
    consumer
  }*/
  

  //获取访问kafka获取分区数
/*  def setFromOffsets(list: ListBuffer[(String, Int, Long)]): Map[TopicPartition, Long] = {
    val hbase=HbaseConn
    val conn=hbase.createConnection()
    val partion=1
    var fromOffsets: Map[TopicPartition, Long] = Map()
    breakable {
      while (true) {
        val rowKey = "meteorological1" + "_meteorological" + partion.toString
        val result = hbase.queryRecord(conn, rowKey)
        for (kv<-result.rawCells()){
          //val value=new String(kv.getValueArray, "UTF-8")
          val tp = new TopicPartition(kv._1, kv._2)
          fromOffsets += (tp -> offset._3)
        }
        if (result.isEmpty){break()}
      }
    }
    hbase.closeConnection(conn)
    fromOffsets
  }*/

  def main(args: Array[String]): Unit = {
    //指定kafka组名
    val group="meteorological1"
    //指定topic名字
    val topic="meteorological"
    //指定kafka的broker地址
    val brokerList="admin.ambari.com:6667"
    //创建topic结合，sparkstreaming可以同时消费多个topic
    val topics:Set[String]=Set(topic)
    //创建sparkconf
    val conf=new SparkConf().setAppName("KafkaDirectWordCount")
    //创建sparkstreaming，同时设置间隔时间为2S
    val ssc=new StreamingContext(conf,Duration(2000))

    //准备kafka的参数
    val kafkaParams = Map(
      "bootstrap.servers" -> brokerList,
      "group.id" -> group,
      "key.deserializer" -> classOf[StringDeserializer],   //序列化
      "value.deserializer" -> classOf[StringDeserializer], //序列化
      //从头开始读取数据
      "auto.offset.reset" -> "earliest" //kafka.api.OffsetRequest.SmallestTimeString
    )

    var mOffset: Long = 0L

    //println(kafka.api.OffsetRequest.SmallestTimeString)

    //获取offset

    //var fromOffsets: Map[TopicAndPartition, Long] = Map()


    //var fromOffsets: Map[TopicPartition, Long] = Map()
    //for (offset <- list) {
    //  val tp = new TopicPartition(offset._1, offset._2)
    //  fromOffsets += (tp -> offset._3)
    //}

    var newOffset: Map[TopicPartition, Long] = Map()
    val tp=new TopicPartition("meteorological", 1)
    val offsetVal=1001
    newOffset += (tp->offsetVal.toLong)

    //val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message())

    //val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams,topics)
    //val kafkaStream = KafkaUtils.createDirectStream(ssc,kafkaParams,topics)
    val kafkaStream = KafkaUtils.createDirectStream(ssc,PreferConsistent,ConsumerStrategies.Assign[String,String](newOffset.keys.toList, kafkaParams,newOffset))



    //val kafkaStream = KafkaUtils.createDirectStream(ssc,PreferConsistent,Subscribe[String, String](topics, kafkaParams))
    //偏移量的范围
    //var offsetRanges = Array[OffsetRange]()
    //println(kafkaStream.map(_._2))//返回tpl第二个元素，注意，tuple是从1开始的
    //存储kafka offset
    kafkaStream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      //println(offsetRanges)
      rdd.foreachPartition{iter=>
        val o:OffsetRange =offsetRanges(TaskContext.get.partitionId)
        val hbase=HbaseConn
        val conn=hbase.createConnection()
        val times=System.currentTimeMillis() //获取时间戳,可以使用new Date()，但是new Date()本质也是调用System.currentTimeMillis();长度为13位例：1590936908866
        val rowKey=(group+"_"+o.topic+"_"+o.partition)
        val value:Map[String,String]=Map("topic"->o.topic,"partition"->o.partition.toString,"fromOffset"->o.fromOffset.toString,"untilOffset"->o.untilOffset.toString)
        hbase.updateRecord(conn,rowKey,value)
        hbase.closeConnection(conn)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        println("kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk")
      }
    }

    println(kafkaStream.map(record => (record.key, record.value)).saveAsTextFiles("file:///c:/data/",suffix = "meteorological"))

    //启动sparksteaming程序
    ssc.start()

    //等待并终止
    ssc.awaitTermination()

  }
}
