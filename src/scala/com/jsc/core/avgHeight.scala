package com.jsc.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jiasichao on 2018/4/22.
  */
object avgHeight {

  def main(args: Array[String]): Unit = {
    val sf = new SparkConf();
    //数据 ID+" "+性别+" "+身高
    //需求统计男女人数、map(x =>(x.split(" ")(1),1)).reduceByKey(_+_)
    // 男性中最高和最低身高map(x =>x.split(" ")).map(x=>(x_3,x_2,x_1)).sortByKey(false).take(1)
    //女性中的最高和最低身高

    //RDD==>MRDD+FRDD
    //MAX MIN
    //MAX MIN

    val sc = new SparkContext(sf);

    sc.stop();

  }

}
