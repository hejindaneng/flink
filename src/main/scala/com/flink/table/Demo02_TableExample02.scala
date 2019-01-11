package com.flink.table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.sinks.CsvTableSink
/**
  * Package: com.flink.table
  * Description： TODO
  * Author: Hejin
  * Date: Created in 2019/1/7 15:49
  * Company: 公司
  * Copyright: Copyright (c) 2017
  * Version: 0.0.1
  * Modified By: 
  */
object Demo02_TableExample02 {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    val tableEnvironment = TableEnvironment.getTableEnvironment(environment)
    val data1 = environment.fromCollection(
      Seq(
        Order(1, "beer", 2),
        Order(1, "football", 3),
        Order(4, "basketball", 1)
      )
    )
    val data2 = environment.fromCollection(
      Seq(
        Order(2, "beer", 3),
        Order(2, "football", 1)
      )
    )
    tableEnvironment.registerDataStream("Table1",data1)
    tableEnvironment.registerDataStream("Table2",data2)
    val result = tableEnvironment.sqlQuery("select * from Table1 where amount >= 2 union all select * from Table2 where amount <= 2")
    result.writeToSink(new CsvTableSink("qwe"))
    environment.execute()
  }
}

case class Order(id: Int, product_name: String,amount : Int)