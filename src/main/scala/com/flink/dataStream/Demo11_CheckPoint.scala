package com.flink.dataStream

import java.util

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

/**
  * Package: com.flink.dataStream
  * Description： TODO
  * Author: Hejin
  * Date: Created in 2019/1/7 10:49
  * Company: 公司
  * Copyright: Copyright (c) 2017
  * Version: 0.0.1
  * Modified By: 
  */
object Demo11_CheckPoint {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStateBackend(new FsStateBackend("hdfs://nod01:9000/checkpoint0000000"))
    environment.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    environment.getCheckpointConfig.setCheckpointInterval(6000)
    environment.getCheckpointConfig.setCheckpointTimeout(10000)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val sourceData = environment.addSource(new SourceData)
    val timeSourceData = sourceData.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Event] {
      override def getCurrentWatermark: Watermark = {
        new Watermark(System.currentTimeMillis())
      }

      override def extractTimestamp(t: Event, l: Long): Long = System.currentTimeMillis()
    })
    val groupData = timeSourceData.keyBy(0)
    val windowData: WindowedStream[Event, Tuple, TimeWindow] = groupData.window(SlidingEventTimeWindows.of(Time.seconds(4),Time.seconds(1)))
    val result = windowData.apply(new FuncWindow)
    result.print()
    environment.execute()
  }
}

class FuncWindow extends WindowFunction[Event,Long,Tuple,TimeWindow] with ListCheckpointed[State] {
  private var count:Long = 0
  private var totalCount:Long = 0
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Event], out: Collector[Long]): Unit = {
    for (env <- input) {
      count += 1
    }
    totalCount += count
    out.collect(count)
  }

  override def snapshotState(l: Long, l1: Long): util.List[State] = {
    val states = new util.ArrayList[State]()
    val state = new State
    states.add(state)
    states
  }

  override def restoreState(list: util.List[State]): Unit = {
    val state = list.get(0)
    totalCount = state.getState
  }
}
class State extends Serializable{
  private var count = 0L
  def getState = count
  def setState(s:Long)  = count = s
}
class SourceData extends RichSourceFunction[Event] {
  private var count = 0
  private var isRunning = true
  private val info = "这是一个测试"
  override def run(sourceContext: SourceFunction.SourceContext[Event]): Unit = {
    while(isRunning) {
      for (i <- 0 until 10000){
        sourceContext.collect(Event(i,"hello"+i,info,count))
      }
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}

case class Event(id: Long, name: String, info: String, count: Int)