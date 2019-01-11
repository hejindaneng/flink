package com.flink.dataSet

import java.lang

import org.apache.flink.api.common.functions.{GroupCombineFunction, GroupReduceFunction}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, GroupedDataSet}
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

/**
  * Package: com.flink.demo.dataSet
  * Description： TODO
  * Author: Hejin
  * Date: Created in 2019/1/4 10:04
  * Company: 公司
  * Copyright: Copyright (c) 2017
  * Version: 0.0.1
  * Modified By: 
  */
object Demo08_CustomFunc {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val data: DataSet[List[(String, Int)]] = environment.fromElements(List(("java", 1), ("scala", 1), ("java", 1)))
    val flatMap_data: DataSet[(String, Int)] = data.flatMap(line => line)
    val group_data: GroupedDataSet[(String, Int)] = flatMap_data.groupBy(line => line._1)
    val result: DataSet[(String, Int)] = group_data.reduceGroup(new Custom_func())
    result.print()
  }

  import collection.JavaConverters._
  class Custom_func() extends GroupReduceFunction[(String, Int), (String, Int)] with GroupCombineFunction[(String, Int), (String, Int)] {
    override def reduce(iterable: lang.Iterable[(String, Int)], collector: Collector[(String, Int)]): Unit = {
      for (in <- iterable.asScala) {
        collector.collect((in._1,in._2))
      }
    }

    override def combine(iterable: lang.Iterable[(String, Int)], collector: Collector[(String, Int)]): Unit = {
      var num = 0
      var s = ""
      for (in <- iterable.asScala) {
        num += in._2
        s = in._1
      }
      collector.collect((s,num))
    }
  }

}
