package com.jsc.core

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jiasichao on 2018/4/24.
  */
object FavoriteTeacher5 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FavoriteTeacher").setMaster("local[2]")

    val sc = new SparkContext(conf)

    val textFile: RDD[String] = sc.textFile("/Users/jiasichao/miaozhen/data/tmp/subjectTecher")

    val lines: RDD[(String, String)] = textFile.map(line => {
      val fields = line.split(" ")
      (fields(0), fields(1))
    })
    val techerValues: RDD[((String, String), Int)] = lines.map((_,1)).reduceByKey(_+_).sortBy(_._2,false)
    val groupBySubject: RDD[(String, Iterable[((String, String), Int)])] = techerValues.groupBy(_._1._1)
    val values: RDD[(String, List[((String, String), Int)])] = groupBySubject.mapValues(_.toList.sortBy(_._2).reverse.take(2))
    println(values.collect().toBuffer)
    sc.stop()
  }
}
