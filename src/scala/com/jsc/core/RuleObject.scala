package com.jsc.core

import java.net.InetAddress

/**
  * Created by jiasichao on 2018/5/20.
  */
//object RuleObject  extends Serializable {
//
//  val rulesMap = Map("hadoop" ->1,"spark"->2)
//}



//第三种方式，希望Rules在Executor中被初始化，就不比实现序列化接口

object RuleObject{

  val rulesMap = Map("hadoop" ->1, "spark" ->2)

  val hostname = InetAddress.getLocalHost.getHostName

  println(hostname+"@@@@@@@@@@@@@@@@@@@@@@@")

}