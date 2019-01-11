package com.flink.table

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Table, TableEnvironment}

/**
  * Package: com.flink.table
  * Description： TODO
  * Author: Hejin
  * Date: Created in 2019/1/7 16:24
  * Company: 公司
  * Copyright: Copyright (c) 2017
  * Version: 0.0.1
  * Modified By: 
  */
object Demo03_TableToStream {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnvironment = TableEnvironment.getTableEnvironment(environment)
    val data = List(
      Project(1L,1,"Hello"),
      Project(2L,2,"Hello"),
      Project(3L,3,"Hello"),
      Project(4L,4,"Hello"),
      Project(5L,5,"Hello"),
      Project(6L,6,"Hello World"),
      Project(7L,7,"Hello World"),
      Project(8L,8,"Hello World")
    )
    val collection: DataStream[Project] = environment.fromCollection(data)
    val table: Table = tableEnvironment.fromDataStream(collection)
    val result: DataStream[Project] = tableEnvironment.toAppendStream[Project](table)
    val result1: DataStream[(Boolean, Project)] = tableEnvironment.toRetractStream[Project](table)
    environment.execute()
  }
}
case class Project(id: Long, index: Int, content: String)