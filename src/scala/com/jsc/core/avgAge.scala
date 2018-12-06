package com.jsc.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jiasichao on 2018/4/21.
  */
object avgAge {

  def main(args: Array[String]): Unit = {

    val spf = new SparkConf();

    val sc = new SparkContext(spf);

    //数据格式   ID+" "+age
      //思路
      //1）年龄
      //2)  人数
      //3)  年龄相加/人数

    val dataFile = sc.textFile("hdfs://127.0.0.1:8000/word/words")

    //取出年龄
    val ageData = dataFile.map(x =>x.split(" ")(1))
    //人数
    val count = dataFile.count()

    //年龄相加/人数
    val max = ageData.map(x => x.toInt).reduce((_+_))


    sc.stop();
  }

}
