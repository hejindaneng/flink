package com.flink.dataSet

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, GroupedDataSet}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
/**
  * Package: com.flink.demo.dataSet
  * Description： TODO
  * Author: Hejin
  * Date: Created in 2019/1/4 16:04
  * Company: 公司
  * Copyright: Copyright (c) 2017
  * Version: 0.0.1
  * Modified By: 
  */
object Demo15_Source {
  def main(args: Array[String]): Unit = {
    val environment = ExecutionEnvironment.getExecutionEnvironment
    // TODO: 读取本地文件
/*    val local_file_data: DataSet[String] = environment.readTextFile("helloWorld.txt")
    //    val hdfs_file_data = environment.readTextFile("hdfs://node01:9000/helloWorld.txt")
        val data: DataSet[String] = local_file_data.flatMap(line => line.split("\\W+"))
    val tuple_data: DataSet[(String, Int)] = data.map(line => (line,1))
    val group_data: GroupedDataSet[(String, Int)] = tuple_data.groupBy(0)
    group_data.reduce((x,y) => (x._1,x._2+y._2)).print()*/
    // TODO: 读取csv文件
   /* val csv_data = environment.readCsvFile[(String, String, String, String, String, Int, Int, Int)](
      filePath = "data2.csv",
      lineDelimiter = "\n",
      fieldDelimiter = ",",
      lenient = false,
      ignoreFirstLine = true,
      includedFields = Array(0, 1, 2, 3, 4, 5, 6, 7)
    )*/
    // TODO: 通过递归的方式遍历文件
  /*  val config = new Configuration
    config.setBoolean("recursive.file.enumeration",true)
    environment.readTextFile("test/data/zookeeper").withParameters(config)*/
    // TODO: 读取压缩文件

  }
}
