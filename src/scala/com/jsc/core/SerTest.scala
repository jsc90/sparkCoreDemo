package com.jsc.core

import java.net.InetAddress

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jiasichao on 2018/5/20.
  */
object SerTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("serTest")
    val sc  = new SparkContext(conf)
    //闭包   在driver被实例化
//    val rules = new Rules

    //一个executor里面只有一个实例化rules
//    val rules = RuleObject

    val lines: RDD[String] = sc.textFile(args(0))

    val r = lines.map(word =>{
      //在map的函数中创建一个rules实例  （太浪费资源）
      //val rules = new Rules
      val hostName = InetAddress.getLocalHost.getHostName
      val threadName = Thread.currentThread().getName
      //Rules.rulesMap 在哪一端初始化
      //Rules的实际使用在executor中

      (hostName,threadName,RuleObject.rulesMap.getOrElse(word,0),RuleObject.toString)

    })

    r.saveAsTextFile(args(1))

    sc.stop()
  }

}
