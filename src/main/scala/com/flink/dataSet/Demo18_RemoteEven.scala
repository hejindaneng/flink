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
object Demo18_RemoteEven {
  def main(args: Array[String]): Unit = {
//    val environment = ExecutionEnvironment.createLocalEnvironment()
    val environment = ExecutionEnvironment.createRemoteEnvironment("node01",8081,"target")
    val path = "hdfs://node01:9000/readme.txt"
    val filedata = environment.readTextFile(path)
    val flatdata = filedata.flatMap(line => line.split("\\W+"))
    val tuple_data = flatdata.map(line => (line,1))
    val group_data = tuple_data.groupBy(line => line._1)
    val result = group_data.reduce((x,y) => (x._1,x._2+y._2))
    result.writeAsText("hdfs://node01:9000/remote")
    environment.execute()
  }
}
