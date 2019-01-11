package com.flink.dataStream

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
  * Package: com.flink.dataStream
  * Description： TODO
  * Author: Hejin
  * Date: Created in 2019/1/7 9:19
  * Company: 公司
  * Copyright: Copyright (c) 2017
  * Version: 0.0.1
  * Modified By: 
  */
object Demo09_MySqlSource {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val result = environment.addSource(new Sql)
    result.print()
    environment.execute()
  }
}

class Student(stu_id: Int, stu_name: String, stu_addr: String, stu_sex: String) {
  override def toString: String = {
    "stu_di" + stu_id + "stu_name" + stu_name + "stu_addr" + stu_addr + "stu_sex" + stu_sex
  }
}

class Sql extends RichSourceFunction[Student] {
  var connection: Connection = null
  var ps: PreparedStatement = null

  override def open(parameters: Configuration): Unit = {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://node01:3306/test"
    val username = "root"
    val password = "root"
    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)
  }

  override def run(sourceContext: SourceFunction.SourceContext[Student]): Unit = {
    val sql = "select * from Student"
    ps = connection.prepareStatement(sql)
    val rs: ResultSet = ps.executeQuery()
    while (rs.next()) {
      val stu_id: Int = rs.getInt("stu_id")
      val stu_name: String = rs.getString("stu_name")
      val stu_addr = rs.getString("stu_addr")
      val stu_sex: String = rs.getString("stu_sex")
      val student = new Student(stu_id, stu_name, stu_addr, stu_sex)
      sourceContext.collect(student)
    }
  }

  override def cancel(): Unit = {

  }

  override def close(): Unit = {
    if (connection != null) {
      connection.close()
    }
    if (ps != null) {
      ps.close()
    }
  }
}