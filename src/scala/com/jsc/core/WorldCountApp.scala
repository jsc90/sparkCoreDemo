package com.jsc.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jiasichao on 2018/4/19.
  */
object WorldCountApp {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("ScalaWordCount")

    val sc = new SparkContext(sparkConf)

    val textfile = sc.textFile(args(0))

    val wc = textfile.flatMap(line =>line.split("\t")).map((_,1)).reduceByKey(_+_)

    val sorted = wc.map(x => (x._2,x._1)).sortByKey().map(x => (x._2,x._1))

    sorted.saveAsTextFile(args(1))

    sc.stop();
  }

}
