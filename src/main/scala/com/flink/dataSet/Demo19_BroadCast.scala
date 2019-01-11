package com.flink.dataSet

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Package: com.flink.demo.dataSet
  * Description： TODO
  * Author: Hejin
  * Date: Created in 2019/1/4 20:44
  * Company: 公司
  * Copyright: Copyright (c) 2017
  * Version: 0.0.1
  * Modified By: 
  */
object Demo19_BroadCast {
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
    val result = input2.map(new RichMapFunction[(Int, String), ArrayBuffer[(Int, String, String, Double)]] {
      var broadcast: mutable.Buffer[(Int, String, Double)] = null

      override def open(parameters: Configuration): Unit = {
        import collection.JavaConverters._
        broadcast = this.getRuntimeContext.getBroadcastVariable[(Int, String, Double)]("input1").asScala
      }

      override def map(in: (Int, String)): ArrayBuffer[(Int, String, String, Double)] = {
        val array = broadcast.toArray
        val newArray = new mutable.ArrayBuffer[(Int, String, String, Double)]()
        var tuple: (Int, String, String, Double) = null
        var index = 0
        while (index < array.size) {
          if (in._1 == array(index)._1) {
            tuple = (in._1, in._2, array(index)._2, array(index)._3)
            newArray += tuple
          }
          index = index + 1
        }
        newArray
      }
    }).withBroadcastSet(input1, "input1")
    print(result.collect()) }
}
