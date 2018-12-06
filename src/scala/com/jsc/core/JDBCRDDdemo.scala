package com.jsc.core

import java.sql.{Connection, DriverManager}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jiasichao on 2018/5/5.
  *
  * 将数据库当中的数据用RDD进行计算
  */
object JDBCRDDdemo {

  val getConnection = () =>{
    DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata","root","123456")
  }

//  val getConnection:Connection = {
//    DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata","root","123456")
//  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JDBCRDDdemo").setMaster("local[2]")

    val sc = new SparkContext(conf)

    val jdbcRdd = new JdbcRDD(sc,
      getConnection,
      "select * from logs where id >=? and id <=?",
      1,
      10,
      2,
      rs =>{
        val id = rs.getInt("id")
        val name = rs.getString("name")
        val age = rs.getInt("age")
        (id,name,age)
      }
    )

    val c = jdbcRdd.collect().toBuffer
    println(c)

    sc.stop()
  }


}
