package com.flink.dataSet

import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}

import scala.collection.mutable
import scala.util.Random
import org.apache.flink.streaming.api.scala._

/**
  * Package: com.flink.demo.dataSet
  * Description： TODO
  * Author: Hejin
  * Date: Created in 2019/1/4 10:45
  * Company: 公司
  * Copyright: Copyright (c) 2017
  * Version: 0.0.1
  * Modified By: 
  */
object Demo09_MostValues {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val data: mutable.MutableList[(Int, String, Double)] = new mutable.MutableList[(Int, String, Double)]
    data.+=((1, "yuwen", 89.0))
    data.+=((2, "shuxue", 92.2))
    data.+=((3, "yingyu", 89.99))
    data.+=((4, "wuli", 98.9))
    data.+=((1, "yuwen", 88.88))
    data.+=((1, "wuli", 93.00))
    data.+=((1, "yuwen", 94.3))
    val input: DataSet[(Int, String, Double)] = environment.fromCollection(data)
    //    val result: AggregateDataSet[(Int, String, Double)] = input.aggregate(Aggregations.MAX,2)
//    val result: AggregateDataSet[(Int, String, Double)] = input.groupBy(1).aggregate(Aggregations.MAX, 2)
//    result.print()
    input.minBy(2,0).print()
  }
}
