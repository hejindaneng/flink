package com.flink.dataStream

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
/**
  * Package: com.flink.dataStream
  * Description： TODO
  * Author: Hejin
  * Date: Created in 2019/1/7 9:37
  * Company: 公司
  * Copyright: Copyright (c) 2017
  * Version: 0.0.1
  * Modified By: 
  */
object Demo10_MySqlSink {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val data = environment.fromElements(Student1(10 , "zhangsan" ,"shanghai","male"))
    data.addSink(new SqlSink)
    environment.execute()
  }
}
case class Student1(stu_id: Int, stu_name: String, stu_addr: String, stu_sex: String) {
  override def toString: String = {
    "stu_di" + stu_id + "stu_name" + stu_name + "stu_addr" + stu_addr + "stu_sex" + stu_sex
  }
}

class SqlSink extends RichSinkFunction[Student1] {
  var connection:Connection = null
  var ps:PreparedStatement = null
  override def open(parameters: Configuration): Unit = {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://node01:3306/test"
    val username = "root"
    val password = "root"
    Class.forName(driver)
    connection =  DriverManager.getConnection(url,username,password)
  }

  override def invoke(value: Student1): Unit = {
    val sql = "insert into Student values(?,?,?,?)"
    ps = connection.prepareStatement(sql)
    ps.setInt(1,value.stu_id)
    ps.setString(2,value.stu_name)
    ps.setString(3,value.stu_addr)
    ps.setString(4,value.stu_sex)
    ps.executeUpdate()
  }
  override def close(): Unit = {
    if(connection != null) {
      connection.close()
    }
    if (ps != null) {
      ps.close()
    }
  }
}