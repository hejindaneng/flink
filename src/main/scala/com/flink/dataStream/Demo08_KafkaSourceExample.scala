package com.flink.dataStream

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.streaming.api.scala._

/**
  * Package: com.flink.dataStream
  * Description： TODO
  * Author: Hejin
  * Date: Created in 2019/1/6 17:51
  * Company: 公司
  * Copyright: Copyright (c) 2017
  * Version: 0.0.1
  * Modified By: 
  */
object Demo08_KafkaSourceExample {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val zkCluster = "node01:2181,node02:2181,node03:2181"
    val kafkaCluster = "node01:9092,node02:9092,node03:9092"
    val kafkaTopic = "test"
    val sinkKafkaTopic = "test1"

    val properties = new Properties()
    properties.setProperty("zookeeper.connect",zkCluster)
    properties.setProperty("bootstrap.servers",kafkaCluster)
    properties.setProperty("group.id",kafkaTopic)

    val kafkaConsumer: FlinkKafkaConsumer09[String] = new FlinkKafkaConsumer09[String](kafkaTopic,new SimpleStringSchema(),properties)
    val source: DataStream[String] = environment.addSource(kafkaConsumer)
    
  }
}
