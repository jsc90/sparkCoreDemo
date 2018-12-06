package com.jsc.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jiasichao on 2018/4/21.
  */
object PageViewsApp {


  def main(args: Array[String]): Unit = {
      val sparkConf = new SparkConf()

      val sc = new SparkContext(sparkConf)

      val textFile = sc.textFile(args(0))

      //用户
        //使用tab分隔，split("\t")
        //5字段 useId  ==>splits(5)  (userid,1)
       val pageView = textFile.map(x => (x.split("\t")(5),1))
      //访问量
        //reduceByKey(_+_) ==> (userid,40)
        val userIds = pageView.reduceByKey(_+_);
      //top N
        //反转  wc.map(x => (x._2,x._1)).sortByKey().map(x => (x._2,x._1))
        //sordBy(_._2)


      sc.stop();
  }
}
