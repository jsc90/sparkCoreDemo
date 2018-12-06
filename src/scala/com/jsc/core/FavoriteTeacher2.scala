package com.jsc.core

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.immutable.HashMap

/**
  * Created by jiasichao on 2018/4/24.
  */
object FavoriteTeacher2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FavoriteTeacher").setMaster("local[2]")

    val sc = new SparkContext(conf)

    val lines = sc.textFile(args(0))

    val subjectAndTeacher = lines.map(line => {
      val url = new URL(line)
      val host = url.getHost
      val subject = host.substring(0,host.indexOf("."))
      val teacher = url.getPath.substring(1)

      (subject , teacher)

    })

    //先聚合
    val reduced: RDD[((String, String), Int)] = subjectAndTeacher.map((_,1)).reduceByKey(_+_)

    //缓存一下
    reduced.cache();
    //采样 有多少学科
    val subjects = reduced.map(_._1._1).distinct().collect()

    //定义分区器
    val subPartitioner = new SubjectPartitioner(subjects)

    val reducedby: RDD[(String, (String, Int))] = reduced.map(t =>(t._1._1,(t._1._2,t._2)))

    //按照自定义分区器规则shuffle
    val partitioned = reducedby.partitionBy(subPartitioner)

    val result = partitioned.mapPartitions(_.toList.sortBy(_._2._2).reverse.take(2).iterator)

    println(result.collect())

    sc.stop()
  }
}

class SubjectPartitioner(subject:Array[String]) extends Partitioner {
  //定义分区规则
  var rules = new HashMap[String, Int]()
  var i = 1
  for(sub <- subject){
    rules += (sub -> i)
    i += 1
  }

  //有几个分区
  override def numPartitions: Int = subject.length

  //根据传入的key 返回具体的分区。
  override def getPartition(key: Any): Int = {
    val k = key.toString
    rules.getOrElse(k,0 )
  }
}
