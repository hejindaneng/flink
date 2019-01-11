package com.flink.dataStream

import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
/**
  * Package: com.flink.dataStream
  * Description： TODO
  * Author: Hejin
  * Date: Created in 2019/1/6 17:30
  * Company: 公司
  * Copyright: Copyright (c) 2017
  * Version: 0.0.1
  * Modified By: 
  */
object Demo06_ConnectExample {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val elements: DataStream[Int] = environment.fromElements(1,3,5,7)
    val str_elements: DataStream[String] = elements.map(line => line+ "x")
    val connectData: ConnectedStreams[Int, String] = elements.connect(str_elements)
    val result = connectData.map {
      new CoMapFunction[Int, String, String] {
        override def map1(in1: Int): String = {
          in1 + "+++++"
        }

        override def map2(in2: String): String = {
          in2
        }
      }
    }
    result.print()
    environment.execute()
  }
}
