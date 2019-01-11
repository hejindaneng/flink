package com.flink.dataSet

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala._
/**
  * Package: com.flink.demo.dataSet
  * Description： TODO
  * Author: Hejin
  * Date: Created in 2019/1/4 17:18
  * Company: 公司
  * Copyright: Copyright (c) 2017
  * Version: 0.0.1
  * Modified By: 
  */
object Demo16_Sink {
  def main(args: Array[String]): Unit = {
    val environment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = environment.fromElements(Map(1 -> "spark",2 -> "flink"))
    ds1.setParallelism(1).writeAsText("aa",WriteMode.OVERWRITE)
    environment.execute()
  }
}
