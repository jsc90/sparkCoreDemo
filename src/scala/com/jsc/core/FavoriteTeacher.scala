package com.jsc.core

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jiasichao on 2018/4/24.
  */
object FavoriteTeacher {

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

    //聚合
    val r = subjectAndTeacher.map((_,1)).reduceByKey(_+_).sortBy(_._2,false)
    //分组
    val groupd = r.groupBy(_._1._1)

    //内部排序（二次排序）
    groupd.mapValues(_.toList.sortBy(_._2).reverse.take(2))
    sc.stop()
  }
}
