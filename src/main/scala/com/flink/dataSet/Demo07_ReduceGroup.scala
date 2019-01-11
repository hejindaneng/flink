package com.flink.dataSet

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, GroupedDataSet}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
/**
  * Package: com.flink.demo.dataSet
  * Description： TODO
  * Author: Hejin
  * Date: Created in 2018/12/29 22:01
  * Company: 公司
  * Copyright: Copyright (c) 2017
  * Version: 0.0.1
  * Modified By: 
  */
object Demo07_ReduceGroup {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val data: DataSet[List[(String, Int)]] = environment.fromElements(List(("java",1),("scala",1),("java",1)))
    val flatMap_data: DataSet[(String, Int)] = data.flatMap(line => line)
    val group_data: GroupedDataSet[(String, Int)] = flatMap_data.groupBy(line => line._1)
    val result: DataSet[(String, Int)] = group_data.reduceGroup {
      (input: Iterator[(String, Int)], output: Collector[(String, Int)]) =>
        val t_data = input.reduce((x, y) => (x._1, x._2 + y._2))
        output.collect(t_data)
    }
    result.print()
  }
}
