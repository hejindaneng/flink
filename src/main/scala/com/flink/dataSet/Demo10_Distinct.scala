package com.flink.dataSet

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

import scala.collection.mutable
import org.apache.flink.streaming.api.scala._

import scala.util.Random
/**
  * Package: com.flink.demo.dataSet
  * Description： TODO
  * Author: Hejin
  * Date: Created in 2019/1/4 11:00
  * Company: 公司
  * Copyright: Copyright (c) 2017
  * Version: 0.0.1
  * Modified By: 
  */
object Demo10_Distinct {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val data: mutable.MutableList[(Int, String, Double)] = new mutable.MutableList[(Int, String, Double)]
    data.+=((1, "yuwen", 89.0))
    data.+=((2, "shuxue", 92.2))
    data.+=((3, "yingyu", 89.99))
    data.+=((4, "wuli", 98.9))
    data.+=((5, "yuwen", 88.88))
    data.+=((6, "wuli", 93.00))
    data.+=((7, "yuwen", 94.3))
    val input: DataSet[(Int, String, Double)] = environment.fromCollection(Random.shuffle(data))
    input.distinct(1).print()
  }
}
