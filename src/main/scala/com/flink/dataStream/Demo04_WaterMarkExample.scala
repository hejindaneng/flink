package com.flink.dataStream

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
/**
  * Package: com.flink.dataStream
  * Description： TODO
  * Author: Hejin
  * Date: Created in 2019/1/5 21:12
  * Company: 公司
  * Copyright: Copyright (c) 2017
  * Version: 0.0.1
  * Modified By: 
  */
object Demo04_WaterMarkExample {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.setParallelism(1)
    val textStream = environment.socketTextStream("localhost",9999)
    val dataInfo = textStream.map {
      line =>
        val array = line.split(",")
        SalePrice1(array(0).trim.toLong, array(1).trim, array(2).trim, array(3).trim.toDouble)
    }
    val timestampData: DataStream[SalePrice1] = dataInfo.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[SalePrice1] {
      var currentMaxWatermark: Long = 0
      val maxWatermarkValue = 2000L
      var wm: Watermark = null
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

      override def getCurrentWatermark: Watermark = {
        wm = new Watermark(currentMaxWatermark - maxWatermarkValue)
        wm
      }

      override def extractTimestamp(element: SalePrice1, previousElementTimestamp: Long): Long = {
        val timestamp = element.timestamp
        currentMaxWatermark = Math.max(timestamp, currentMaxWatermark)
        println("timestamp:(" + element.timestamp + "," + element.boosName + "," + element.productName + "," + element.price)
        timestamp
      }
    })
    val groupData = timestampData.keyBy(line => line.productName)
    val windowData: WindowedStream[SalePrice1, String, TimeWindow] = groupData.timeWindow(Time.seconds(3))
    val result = windowData.apply(new MyFunction)
    result.print()
    environment.execute()
  }
}
case class SalePrice1(timestamp:Long,boosName:String,productName:String,price:Double)

class MyFunction extends WindowFunction[SalePrice1,SalePrice1,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[SalePrice1], out: Collector[SalePrice1]): Unit = {
    val toArray = input.toArray
    val take = toArray.sortBy(line => line.price).reverse.take(1)
    for (info <- take) {
      out.collect(info)
    }
  }
}