package test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]) {

    // 创建一个具有两个工作线程（working thread），同时appName设置为"spark wordcount"
    val conf=new SparkConf().setAppName("spark_wordcount").setMaster("local[2]")
    //创建sparkcontext对象
    val sc=new SparkContext(conf)
    //val txtFile="file:///C:/Users/zhaow/IdeaProjects/Bigdata/data/data.csv"
    val txtFile="///data/data.csv"
    //创建textFile RDD
    var txtData=sc.textFile(txtFile)
    //缓存txtData RDD
    txtData.cache()
    //对RDD计数
    txtData.count()
    //使用split切分每行数据，使用map算子将每个单词的计数值初始化为1，使用reduceByKey算子开始计数
    val wcData=txtData.flatMap{line=>line.split(" ")}.map{word=>(word,1)}.reduceByKey(_+_)
    //使用collect() action算子，开始执行RDD
    val collect=wcData.collect()
    println(collect)
    //输出数组数据(a: String,n:Int)=>{println(a+':'+n)}
    //collect.foreach((word:String,num:Int)=>{println(word+':'+num)})
    collect.foreach((words)=>println(words._1+':'+words._2))

  }


}
