import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.hadoop.hbase.TableName
import org.apache.flink.streaming.api.scala._
/**
  * Package: 
  * Description： TODO
  * Author: Hejin
  * Date: Created in 2019/1/9 10:43
  * Company: 公司
  * Copyright: Copyright (c) 2017
  * Version: 0.0.1
  * Modified By: 
  */
object DataExtraction {
  val zkCluster = "node01,node02,node03"
  val zkPort = "2181"
  val kafkaCluster = "node01:9092,node02:9092,node03"
  val topicName = "myCanal"
  private val tableName: TableName = TableName.valueOf("canal")
  val columnFamily = "info"

  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    environment.setStateBackend(new FsStateBackend("hdfs://node01:9000/flink-checkpoint"))
    environment.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    environment.getCheckpointConfig.setCheckpointInterval(6000)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.getConfig.setAutoWatermarkInterval(2000)
    System.setProperty("hadoop.home.dir","/")
    val properties = new Properties()
    properties.setProperty("zookeeper.connect",zkCluster)
    properties.setProperty("bootstrap.servers",kafkaCluster)
    properties.setProperty("group.id",topicName)
    val kafka09 = new FlinkKafkaConsumer09[String](topicName,new SimpleStringSchema(),properties)
    val source = environment.addSource(kafka09)
    val data = source.map {
      line =>
        val values = line.split("##")
        val length = values.length
        val logfileName = if (length > 0) values(0) else ""
        val logfileOffset = if (length > 1) values(1) else ""
        val dbName = if (length > 2) values(2) else ""
        val tableName = if (length > 3) values(3) else ""
        val eventType = if (length > 4) values(4) else ""
        val columns = if (length > 5) values(5) else ""
        val rowNum = if (length > 6) values(6) else ""
        Canal(logfileName, logfileOffset, dbName, tableName, eventType, columns, rowNum)
    }
    data.print()
    environment.execute()
  }
}

case class Canal(
                logfileName :String,
                logfileOffset:String,
                dbName:String,
                tableName:String,
                eventType:String,
                columns:String,
                rowNum:String
                )
