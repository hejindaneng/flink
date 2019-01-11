package com.flink.dataStream

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
  * Package: com.flink.dataStream
  * Description： TODO
  * Author: Hejin
  * Date: Created in 2019/1/5 17:16
  * Company: 公司
  * Copyright: Copyright (c) 2017
  * Version: 0.0.1
  * Modified By: 
  */
object Demo03_EventExample {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val textStream = environment.socketTextStream("localhost", 9999)
    val dataInfo = textStream.map {
      line =>
        val array = line.split(",")
        SalePrice(array(0).trim.toLong, array(1).trim, array(2).trim, array(3).trim.toDouble)
    }
    //使用已EventTime划分窗口进行窗口的划分
    val timestamps: DataStream[SalePrice] = dataInfo.assignAscendingTimestamps(line => line.timeStamp)
    val groupProduct: KeyedStream[SalePrice, String] = timestamps.keyBy(line => line.productName)
    val windowData: WindowedStream[SalePrice, String, TimeWindow] = groupProduct.timeWindow(Time.seconds(3))
    val maxPrice: DataStream[SalePrice] = windowData.max("price")
    maxPrice.print()
    environment.execute()
  }
}

case class SalePrice(timeStamp: Long, boosName: String, productName: String, price: Double)
