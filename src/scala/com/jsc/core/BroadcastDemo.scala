package com.jsc.core

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jiasichao on 2018/5/3.
  */
object BroadcastDemo {

  //模拟MR的map side join , 有一个小文件,这个小文件要跟一个大文件进行join,如果在默认情况下会产生shuffle。
  //可以将小文件缓存到MAP端，其实就是将小文件数据载入内存

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MapSideJoin").setMaster("local[2]")

    val sc: SparkContext = new SparkContext(conf)

    //创建一个rules
    //RDD方法中传入的函数是在Executor的Task中执行的，Driver会将这个变量发送到每一个Task
    val rules = Map("cn" -> "中国","us"->"美国","jp"->"日本")
    val broadcast: Broadcast[Map[String, String]] = sc.broadcast(rules)

    val lines = sc.textFile("/Users/jiasichao/miaozhen/data/tmp/msj.log")

    val result: RDD[(String,String)] = lines.map(lines => {
      val fields = lines.split(",")
      val name = fields(0)
      val nationCode = fields(1)
      val nationName = broadcast.value(nationCode)
      (name, nationName)

    })

    println(result.collect().toBuffer)

    sc.stop()

  }

}
