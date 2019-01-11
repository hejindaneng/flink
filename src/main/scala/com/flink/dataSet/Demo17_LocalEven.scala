package com.flink.dataSet

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
/**
  * Package: com.flink.demo.dataSet
  * Description： TODO
  * Author: Hejin
  * Date: Created in 2019/1/4 19:35
  * Company: 公司
  * Copyright: Copyright (c) 2017
  * Version: 0.0.1
  * Modified By: 
  */
object Demo17_LocalEven {
  def main(args: Array[String]): Unit = {
//    val environment = ExecutionEnvironment.createLocalEnvironment()
    val environment = ExecutionEnvironment.createCollectionsEnvironment
    val path = "data2.csv"
    val csv_data = environment.readCsvFile[(String, String, String, String, String, Int, Int, Int)](
      filePath = path,
      lineDelimiter = "\n",
      fieldDelimiter = ",",
      lenient = false,
      ignoreFirstLine = true,
      includedFields = Array(0, 1, 2, 3, 4, 5, 6, 7)
    )
    csv_data.groupBy(0,1).first(100).print()
  }
}
