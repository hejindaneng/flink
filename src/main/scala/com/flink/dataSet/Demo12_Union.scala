package com.flink.dataSet

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

/**
  * Package: com.flink.demo.dataSet
  * Description： TODO
  * Author: Hejin
  * Date: Created in 2019/1/4 11:53
  * Company: 公司
  * Copyright: Copyright (c) 2017
  * Version: 0.0.1
  * Modified By: 
  */
object Demo12_Union {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val environment1: DataSet[String] = environment.fromElements("123")
    val environment2: DataSet[String] = environment.fromElements("456")
    val environment3: DataSet[String] = environment.fromElements("123")
    val union = environment1.union(environment2).union(environment3).distinct(line => line)
    union.print()
  }
}
