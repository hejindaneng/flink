package com.flink.dataStream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
/**
  * Package: com.flink.dataStream
  * Description： TODO
  * Author: Hejin
  * Date: Created in 2019/1/5 16:17
  * Company: 公司
  * Copyright: Copyright (c) 2017
  * Version: 0.0.1
  * Modified By: 
  */
object Demo2_TumblingCountWindow {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val textStream = environment.socketTextStream("localhost",9999)
    val data = textStream.map {
      line =>
        val array = line.split(",")
        CountCar(array(0).trim.toInt, array(1).trim.toInt)
    }
    val keyByData = data.keyBy(line => line.sen)
    keyByData.countWindow(3).sum(1).print()
    environment.execute()
  }
}
case class CountCar(sen:Int,carNum:Int)