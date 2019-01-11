package com.flink.dataSet

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

/**
  * Package: com.flink.demo.dataSet
  * Description： TODO
  * Author: Hejin
  * Date: Created in 2019/1/4 12:02
  * Company: 公司
  * Copyright: Copyright (c) 2017
  * Version: 0.0.1
  * Modified By: 
  */
object Demo13_Rebalance {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds: DataSet[Long] = environment.generateSequence(1,3000)
    val data: DataSet[Long] = ds.filter(_ > 780)
    val rebalanced = data.rebalance()
    val countsInPartition: DataSet[(Int, Long)] = rebalanced.map(new RichMapFunction[Long, (Int, Long)] {
      override def map(in: Long): (Int, Long) = {
        (getRuntimeContext.getIndexOfThisSubtask, in)
      }
    })
    countsInPartition.print()
  }
}
