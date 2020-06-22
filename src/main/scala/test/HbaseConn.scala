package test
import org.apache.hadoop.hbase.{Cell, HBaseConfiguration, HColumnDescriptor, HTableDescriptor, NamespaceDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, _}
import org.apache.hadoop.hbase.client.TableDescriptorBuilder
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder
import org.json4s.jackson.Json

import scala.util.parsing.json.JSONObject

object HbaseConn {

  //声明hbase连接信息
  private[this] val zookeeperHost="admin.ambari.com:2181,agent1.ambari.com:2181" //private[this]本类中可以访问
  private[this] val hbasehome="/hbase-unsecure"
  private[this] val tableName="bigdata:kafka"
  private[this] val columnFamily="properties"
  private[this] val column="offsets"

  //创建连接
  def createConnection(): Connection ={//Admin为返回类型
    // Hbase 简要配置以及开启服务
    val hbaseConf =  HBaseConfiguration.create()
    //设置zookeeper连接
    hbaseConf.set("hbase.zookeeper.quorum", zookeeperHost)
    hbaseConf.set("zookeeper.znode.parent",hbasehome)
    val connHbase = ConnectionFactory.createConnection(hbaseConf)
    //val admin: Admin = connHbase.getAdmin()
    //println("连接创建成功!")
    return connHbase
  }

  //关闭连接
  def closeConnection(conn:Connection) :Unit={
    try{
      if(conn!=null){
        conn.close();
      }
    }catch{
      case ex:Exception=>ex.getMessage
    }
  }

  //创建命名空间
  def createNameSpace(nameSpace:String,admin:Admin): Unit={
    //val admin1=createConnection()

    try {
      val namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build
      admin.createNamespace(namespaceDescriptor)
      println("namespace:创建成功!")
    } catch{
      case ex:Exception=>ex.getMessage
        println("namespace已经存在!")
    }
    println(admin.listTableNames())
  }

  // 确认 Hbase 表存在
  def createTable(tabName:String,colFamily:String,admin:Admin) = {
    //val admin=createConnection()
    val tableName = TableName.valueOf(tabName)
    println("我进来了!")
    val isExist = admin.tableExists(tableName)
    // 是否存在表，不存在新建
    if (!isExist) {
      try{
      //构造tableBuilder
      val  tableBuilder= TableDescriptorBuilder.newBuilder(tableName)

      // topic 为 ColumnFamily，创建列
      val columnFamily= ColumnFamilyDescriptorBuilder.of(colFamily)
      tableBuilder.setColumnFamily(columnFamily)

      //构建表
      val htable=tableBuilder.build()
      admin.createTable(htable)
      println("表创建成功:" + htable)
      }catch{
        case ex:Exception=>ex.getMessage
        println("表创建失败")
      }
    }

  }

  //更新hbase记录
  def updateRecord(conn:Connection,rowkey:String,map: Map[String,String]): Unit ={
    val tableName="bigdata:kafka"
    val columnFamily="properties"
    val column="offsets"
    val hTable = conn.getTable(TableName.valueOf(tableName))
    //设置rowkey
    val puts = new Put(rowkey.getBytes());
    //向Put对象中组装数据
    puts.addColumn(columnFamily.getBytes(),column.getBytes(),map.toString.getBytes())
    //写入数据
    try{
    hTable.put(puts)
      println("写入数据成功!")
    }catch {
      case ex:Exception=>ex.getMessage
        println("写入数据失败!")
    }
  }

  //检索，按key检索
  def queryRecord(conn:Connection,rowKey:String): Result  ={
    val table:Table = conn.getTable(TableName.valueOf(tableName))
    val get: Get = new Get(rowKey.getBytes())
    val result: Result = table.get(get)
    /*
    for (rowKv <- result.rawCells()) {
      println("Famiily:" + new String(rowKv.getFamilyArray, rowKv.getFamilyOffset, rowKv.getFamilyLength, "UTF-8"))
      println("Qualifier:" + new String(rowKv.getQualifierArray, rowKv.getQualifierOffset, rowKv.getQualifierLength, "UTF-8"))
      println("TimeStamp:" + rowKv.getTimestamp)
      println("rowkey:" + new String(rowKv.getRowArray, rowKv.getRowOffset, rowKv.getRowLength, "UTF-8"))
      println("Value:" + new String(rowKv.getValueArray, rowKv.getValueOffset, rowKv.getValueLength, "UTF-8"))
    }
    */
    result
  }

  //检索，获取某一行指定“列族:列”的数据
  def queryRowQualifier(conn:Connection,rowKey:String): Result  ={
    val table:Table = conn.getTable(TableName.valueOf(tableName))
    val get: Get = new Get(rowKey.getBytes())
    get.addColumn(columnFamily.getBytes(), column.getBytes())
    val result: Result = table.get(get)
    //for (rowKv <- result.rawCells()) {
      //println("Famiily:" + new String(rowKv.getFamilyArray, rowKv.getFamilyOffset, rowKv.getFamilyLength, "UTF-8"))
      //println("Qualifier:" + new String(rowKv.getQualifierArray, rowKv.getQualifierOffset, rowKv.getQualifierLength, "UTF-8"))
      //println("TimeStamp:" + rowKv.getTimestamp)
      //println("rowkey:" + new String(rowKv.getRowArray, rowKv.getRowOffset, rowKv.getRowLength, "UTF-8"))
      //println("Value:" + new String(rowKv.getValueArray, "UTF-8"))
    //}
     result
  }



  def main(args: Array[String]): Unit = {
    val conn=createConnection()
    val admin=conn.getAdmin()
    createNameSpace("bigdata",admin)
    createTable("bigdata:kafka","properties",admin)
    val offsets:Map[String,String]  = Map("offset" -> "100", "topic" -> "spark")
    updateRecord(conn,"1001",offsets)


    //定义表名称
    val result:Result=queryRecord(conn,"meteorological0")

    //val result2:Result=queryRowQualifier(conn,"meteorological0")

    println(result.getMap().toString)
    closeConnection(conn)

  }

}
