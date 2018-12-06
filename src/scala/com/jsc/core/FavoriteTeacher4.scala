package com.jsc.core

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.immutable.HashMap

/**
  * Created by jiasichao on 2018/4/24.
  */
object FavoriteTeacher4 {

  def main(args: Array[String]): Unit = {

    //read rules from mysql
    val subjects = List("bigdata","java","php")

    val conf = new SparkConf().setAppName("FavoriteTeacher").setMaster("local[2]")

    val sc = new SparkContext(conf)

    val lines = sc.textFile(args(0))
    lines.cache()

    val subjectAndTeacher = lines.map(line => {
      val url = new URL(line)
      val host = url.getHost
      val subject = host.substring(0,host.indexOf("."))
      val teacher = url.getPath.substring(1)

      (subject , teacher)

    })
    subjectAndTeacher.repartitionAndSortWithinPartitions
    //如果内存够大 可以cache
    eacher.cache()

    for(sub <-subjects){
      //不使用list方式排序，而是使用RDD的排序方法
      val bigdataRdd: RDD[(String, String)] = subjectAndTeacher.filter(s=>sub.equals(s._1))
      val result = bigdataRdd.map((_, 1)).reduceByKey(_ + _).sortBy(_._2,false).take(2)

      println(result.toBuffer)
    }



    sc.stop()
  }
}

class SubjectPartitioner2 extends Partitioner {
  //定义分区规则
  //读取学科信息
  val rules = Map("bigdata" ->1,"java"->2,"php"->3)

  //有几个分区
  override def numPartitions: Int = rules.size+1

  //根据传入的key 返回具体的分区。
  override def getPartition(key: Any): Int = {
    val tg = key.asInstanceOf[Tuple2[String,String]]
    rules.getOrElse(tg._1,0 )
  }
}
