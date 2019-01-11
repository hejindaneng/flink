package com.flink.dataSet

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable

/**
  * Package: com.flink.demo.dataSet
  * Description： TODO
  * Author: Hejin
  * Date: Created in 2019/1/4 15:21
  * Company: 公司
  * Copyright: Copyright (c) 2017
  * Version: 0.0.1
  * Modified By:
  */
object Demo14_PartitionByHash {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val data = new mutable.MutableList[(Int,Long,String)]
    data.+= ((1,1L,"Hi"))
    data.+= ((2,2L,"Hello"))
    data.+= ((3,2L,"Hello world"))
    data.+= ((4,3L,"Hello world,how are you?"))
    data.+= ((5,3L,"I am fine."))
    data.+= ((6,4L,"Luke Skywalker"))
    data.+= ((7,4L,"May"))
    data.+= ((8,5L,"the force"))
    data.+= ((9,6L,"be"))
    data.+= ((10,6L,"with you!"))
    val collection = environment.fromCollection(data)
/*    val hash_data = collection.partitionByHash(1)
    val result = hash_data.mapPartition{line => line.map(x =>(x._1 ,x._2,x._3) )}
    result.writeAsText("partitionByHash",WriteMode.OVERWRITE)*/
/*    val range_data = collection.partitionByRange(1)
    val result = range_data.mapPartition{line => line.map(x =>(x._1 ,x._2,x._3) )}
    result.writeAsText("partitionByRange",WriteMode.OVERWRITE)*/
    val pa_data = collection.setParallelism(2)
    val sort_data = pa_data.sortPartition(1,Order.DESCENDING)
    val result = sort_data.mapPartition {
      line => line.map(x => (x._1, x._2, x._3))
    }
    result.writeAsText("sortPartition",WriteMode.OVERWRITE)
    environment.execute()
  }
}
