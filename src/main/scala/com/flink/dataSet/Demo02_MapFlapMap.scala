package com.flink.dataSet

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
/**
  * Package: com.flink.demo.dataSet
  * Description： TODO
  * Author: Hejin
  * Date: Created in 2018/12/29 20:52
  * Company: 公司
  * Copyright: Copyright (c) 2017
  * Version: 0.0.1
  * Modified By: 
  */
object Demo02_MapFlapMap {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val data: DataSet[(String, Int)] = environment.fromElements(("A",1),("B",1),("C",1))
    data.map(line => line._1+line._2).print()
    data.flatMap(line => line._1+line._2).print()
  }
}
