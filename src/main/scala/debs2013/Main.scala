package debs2013

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object Main extends App {
  // Setup environment
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  // Setup source
  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "localhost:9092")

  val kafkaSource = new FlinkKafkaConsumer[String]("test", new SimpleStringSchema(), properties)

  kafkaSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor[String] {
    def extractAscendingTimestamp(eventStr: String): Long = {
      eventStr.split(",")(1).toLong
    }
  })

  // Create topology
  env
    .addSource(kafkaSource)
    .print()


  // Run topology
  env.execute()
}
