package com.flink.dataStream

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
/**
  * Package: com.flink.dataStream
  * Description： TODO
  * Author: Hejin
  * Date: Created in 2019/1/6 17:16
  * Company: 公司
  * Copyright: Copyright (c) 2017
  * Version: 0.0.1
  * Modified By: 
  */
object Demo05_WindowProcessingTime {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val textStream = environment.socketTextStream("localhost",9999)
    val data = textStream.map {
      line =>
        val array = line.split(",")
        SalePrice3(array(0).trim.toLong, array(1).trim, array(2).trim, array(3).trim.toDouble)
    }
    data.keyBy(line => line.productName).timeWindow(Time.seconds(3)).max("price").print()
    environment.execute()
  }
}

case class SalePrice3(timestamp:Long,boosName:String,productName:String,price:Double)
