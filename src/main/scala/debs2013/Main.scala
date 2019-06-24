package debs2013

import java.util.Properties

import debs2013.operators.{BallPossessionChecker, EnrichedEventMap, RawEventMap, UnusedDataFilter}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
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
    .map(new RawEventMap()).setParallelism(1)
    .filter(new UnusedDataFilter()).setParallelism(1)
    .flatMap(new EnrichedEventMap()).setParallelism(1)
    .flatMap(new BallPossessionChecker()).setParallelism(1)
    // optional windowAll here

  // .keyBy((el: RawEvent) => el.timestamp)
    // .window(TumblingEventTimeWindows.of(Time.seconds(5)))
    // .reduce((el1: RawEvent, el2: RawEvent) => {
    //       el1
    // })
    // .writeAsText("/Users/lpraat/develop/scep2019/results/result.txt")


  // Run topology
  env.execute()
}
