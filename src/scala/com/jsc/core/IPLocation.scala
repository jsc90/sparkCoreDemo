package com.jsc.core

import java.sql.{Connection, DriverManager}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by jiasichao on 2018/5/4.
  */
object IPLocation {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("IPLocation").setMaster("local[2]")

    val sc = new SparkContext()

    //指定以后从哪里读取数据
    //1.ip规则数据
    val lines = sc.textFile(args(0))
    val rules: RDD[(Long, Long, String)] = lines.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val provice = fields(6)

      (startNum, endNum, provice)
    })
    val ipRules = rules.collect()
    //广播ip规则
    val broadcast: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(ipRules)

    //2.网站的访问日志
    val accesslog: RDD[String] = sc.textFile(args(1))
    //整理数据
    val provinceAndOne = accesslog.map(line => {
      val fields = line.split("[|]")
      val ipNum = IpTest.ip2Long(fields(1))
      val ipIndex:Int = IpTest.binarySearch(broadcast.value,ipNum)
      var province = ""
      if(ipIndex != -1){
        province =broadcast.value(ipIndex)._3
      }
      (province,1)
    })
    val value = provinceAndOne.reduceByKey(_+_)
//    println(value.collect().toBuffer)

    //将数据保存到数据库中
    //foreach是一个action
//    value.foreach(data =>{
//      IpTest.data2MySQL(data)
//    })
    //=====不好=== ， 每写一条数据创建一个连接，消耗资源太多。



//    val conn : Connection = null
//    这样也不好，一个task就有一个connection 从driver需要序列化

    //一次拿出来一个分区。分区用迭代器引用
    value.foreachPartition(part =>{
      //传入一个分区过去，一个分区有多条数据，一个分区创建一个JDBC链接，写完这个分区的数据再关闭JDBC链接
      IpTest.data2MySQL(part)

    })

    sc.stop()
  }


}


object IpTest{

    def ip2Long(ip:String):Long ={
      val fragments = ip.split("[.]")
      var ipNum = 0l
      for(i <- 0 until fragments.length ){
        ipNum = fragments(i).toLong | ipNum << 8L
      }
      ipNum
    }
  def binarySearch(lines:Array[(Long,Long,String)],ip:Long):Int ={
    var low = 0
    var high = lines.length-1
    while (low <= high){
      val middle = (low + high) /2
      if ((ip >= lines(middle)._1) && ip <= lines(middle)._1){
        return middle
      }
      if(ip < lines(middle)._1){
        high = middle -1
      }else{
        low = middle +1
      }
    }
    -1
  }
  def data2MySQL(part:Iterator[(String,Int)]):Unit ={
    //创建一个JDBC连接
    val conn:Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata","root","123456")
    val prepareStatement = conn.prepareStatement("INSERT INTO access_log values (?,?)")

    //写入数据
    part.foreach(line =>{
      prepareStatement.setString(1,line._1)
      prepareStatement.setInt(2,line._2)
      prepareStatement.executeUpdate()
    })
    prepareStatement.close()
    conn.close()
  }

  def main(args: Array[String]): Unit = {
    val ip ="123.113.96.30"
    val num = ip2Long(ip)
    println(num)
  }
}
