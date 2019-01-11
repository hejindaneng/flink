package com.flink.dataStream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
/**
  * Package: com.flink.dataStream
  * Description： TODO
  * Author: Hejin
  * Date: Created in 2019/1/5 15:47
  * Company: 公司
  * Copyright: Copyright (c) 2017
  * Version: 0.0.1
  * Modified By: 
  */
object Demo1_TumblingTimeWindow {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val textStream = environment.socketTextStream("localhost",9999)
    val data = textStream.map {
      line =>
        val array = line.split(",")
        CarNum(array(0).trim.toInt, array(1).trim.toInt)
    }
    val keyByData = data.keyBy(line=>line.sen)
    val result = keyByData.timeWindow(Time.seconds(10),Time.seconds(5)).sum(1)
    result.print()
    environment.execute()
  }
}
case class CarNum(sen:Int,carNum:Int)
