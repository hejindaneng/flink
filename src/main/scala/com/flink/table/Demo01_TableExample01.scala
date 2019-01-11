package com.flink.table

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.streaming.api.scala._

/**
  * Package: com.flink.table
  * Description： TODO
  * Author: Hejin
  * Date: Created in 2019/1/7 15:23
  * Company: 公司
  * Copyright: Copyright (c) 2017
  * Version: 0.0.1
  * Modified By: 
  */
object Demo01_TableExample01 {
  def main(args: Array[String]): Unit = {
    val environment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnvironment = TableEnvironment.getTableEnvironment(environment)
    val tableSource = CsvTableSource.builder()
      .path("data1.csv")
      .field("id",Types.INT)
      .field("name",Types.STRING)
      .field("age",Types.INT)
      .fieldDelimiter(",")
      .ignoreFirstLine()
      .ignoreParseErrors()
      .lineDelimiter("\r\n")
      .build()
    tableEnvironment.registerTableSource("TableSourceA",tableSource)
    //使用table方式查询
//    val result = tableEnvironment.scan("TableSourceA").select("id,name,age").filter("age<11")
    val result = tableEnvironment.sqlQuery("select id,name,age from TableSourceA where age < 11")
    result.writeToSink(new CsvTableSink("tableSource.csv","----",1,FileSystem.WriteMode.OVERWRITE))
    environment.execute()
  }
}
