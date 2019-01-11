package com.flink.dataStream

import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

/**
  * Package: com.flink.dataStream
  * Description： TODO
  * Author: Hejin
  * Date: Created in 2019/1/5 11:14
  * Company: 公司
  * Copyright: Copyright (c) 2017
  * Version: 0.0.1
  * Modified By: 
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val textStream: DataStream[String] = environment.socketTextStream("localhost",9090)
    val splitData: DataStream[String] = textStream.flatMap(line => line.split("\\W+"))
    val tupleData: DataStream[(String, Int)] = splitData.map(line => (line,1))
    val groupData: KeyedStream[(String, Int), String] = tupleData.keyBy(line => line._1)
    groupData.sum(1).print()
    environment.execute()
  }
}
