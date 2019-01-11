package com.flink.dataSet

import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, GroupedDataSet}
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable
import scala.util.Random

/**
  * Package: com.flink.demo.data1Set
  * Description： TODO
  * Author: Hejin
  * Date: Created in 2019/1/4 11:05
  * Company: 公司
  * Copyright: Copyright (c) 2017
  * Version: 0.0.1
  * Modified By: 
  */
object Demo11_Join {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val data1: mutable.MutableList[(Int, String, Double)] = new mutable.MutableList[(Int,String,Double)]
    //学号---学科---分数
    data1.+=((1, "yuwen", 89.0))
    data1.+=((2, "shuxue", 92.2))
    data1.+=((3, "yingyu", 89.99))
    data1.+=((4, "wuli", 98.9))
    data1.+=((5, "yuwen", 88.88))
    data1.+=((6, "wuli", 93.00))
    data1.+=((7, "yuwen", 94.3))
    val data2: mutable.MutableList[(Int, String)] = new mutable.MutableList[(Int,String)]
    data2.+= ((1,"class_1"))
    data2.+= ((2,"class_2"))
    data2.+= ((3,"class_2"))
    data2.+= ((4,"class_3"))
    data2.+= ((5,"class_3"))
    data2.+= ((6,"class_4"))
    data2.+= ((7,"class_1"))
    val input1: DataSet[(Int, String, Double)] = environment.fromCollection(Random.shuffle(data1))
    val input2: DataSet[(Int, String)] = environment.fromCollection(Random.shuffle(data2))
    val result_join: DataSet[(Int, String, String, Double)] = input2.join(input1).where(0).equalTo(0) {
      (line1, line2) => (line1._1, line1._2, line2._2, line2._3)
    }
    val result_group: GroupedDataSet[(Int, String, String, Double)] = result_join.groupBy(1,2)
    result_group.aggregate(Aggregations.MAX,3).print()
  }
}
