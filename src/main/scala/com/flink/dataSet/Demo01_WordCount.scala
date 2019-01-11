package com.flink.dataSet

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment, GroupedDataSet}
import org.apache.flink.streaming.api.scala._
/**
  * Package: com.flink.demo
  * Description： TODO
  * Author: Hejin
  * Date: Created in 2018/12/29 16:04
  * Company: 公司
  * Copyright: Copyright (c) 2017
  * Version: 0.0.1
  * Modified By:
  */
object Demo01_WordCount {
  def main(args: Array[String]): Unit = {
    //    1.获得一个execution environment
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //    2.加载/创建初始数据
    val text: DataSet[String] = environment.fromElements(
      "Who's there?",
      "I think I hear them. Stand ,ho! Who's there?"
    )
    //    3.指定这些数据的转换
    val flatMap_data: DataSet[String] = text.flatMap(line => line.toLowerCase().split("\\W+"))
    val nonEmpty_data: DataSet[String] = flatMap_data.filter(line => line.nonEmpty)
    val tuple_data: DataSet[(String, Int)] = nonEmpty_data.map(line => (line,1))
    val group_data: GroupedDataSet[(String, Int)] = tuple_data.groupBy(0)
    val total_data: AggregateDataSet[(String, Int)] = group_data.sum(1)
    //    4.指定将计算结果放在哪里
//    total_data.print()
    total_data.writeAsText("hdfs://hadoop01:9000/wordCount")
    //    5.触发程序执行
    environment.execute()
  }
}
