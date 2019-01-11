package com.flink.dataStream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
/**
  * Package: com.flink.dataStream
  * Description： TODO
  * Author: Hejin
  * Date: Created in 2019/1/6 17:39
  * Company: 公司
  * Copyright: Copyright (c) 2017
  * Version: 0.0.1
  * Modified By: 
  */
object Demo07_SplitSelectExample {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    val elements = environment.fromElements(1,2,3,4,5,6,7)
    val dataStream = elements.split {
      (num: Int) =>
        (num % 2) match {
          case 0 => List("event")
          case 1 => List("ssss")
        }
    }
    val result: DataStream[Int] = dataStream.select("event")
    result.print()
    environment.execute()
  }
}
