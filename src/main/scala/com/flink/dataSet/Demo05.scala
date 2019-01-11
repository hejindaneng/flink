package com.flink.dataSet

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment, GroupedDataSet}
import org.apache.flink.streaming.api.scala._

/**
  * Package: com.flink.demo.dataSet
  * Description： TODO
  * Author: Hejin
  * Date: Created in 2018/12/29 20:57
  * Company: 公司
  * Copyright: Copyright (c) 2017
  * Version: 0.0.1
  * Modified By:
  */
object Demo05 {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val data: DataSet[String] = environment.fromElements("A;B;C;D;B;D;C;B;D;A;E;D;C;A;B")
    val map_data = data.map(line => line.split(";"))
    val tuple_data: DataSet[(String, Int)] = map_data.flatMap {
      line => for (index <- 0 until line.length-1) yield (line(index) + "+" + line(index + 1), 1)
    }
    val group_data: GroupedDataSet[(String, Int)] = tuple_data.groupBy(0)
    val result: AggregateDataSet[(String, Int)] = group_data.sum(1)
    result.print()
  }
}
