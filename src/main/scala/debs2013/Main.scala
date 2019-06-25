package debs2013

import java.util.Properties

import debs2013.operators.shot_on_goal.ShotOnGoalChecker
import debs2013.operators.{EnrichedEventMap, RawEventMap, UnusedDataFilter}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object Main extends App {

  // Setup environment
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.setParallelism(1)

  // Setup source
  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "localhost:9092")

  val kafkaSource = new FlinkKafkaConsumer[String]("test", new SimpleStringSchema(), properties)

  kafkaSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor[String] {
    def extractAscendingTimestamp(eventStr: String): Long = {
      eventStr.split(",")(1).toLong
    }
  })

  // TODO add parser and set this option among the others!
  kafkaSource.setStartFromEarliest()


  // TODO name operators
  // TODO add come docs

  // Create topology

  val mainFLow =   env
    .addSource(kafkaSource)
    .map(new RawEventMap())
    .filter(new UnusedDataFilter())
    .flatMap(new EnrichedEventMap())


  mainFLow
    .flatMap(new ShotOnGoalChecker())
    .startNewChain()
    .writeAsText("/Users/lpraat/develop/scep2019/results/shots.txt")


  //mainFLow
   // .flatMap(new BallPossessionChecker()).setParallelism(1)
//    .startNewChain()
   // .writeAsText("/Users/lpraat/develop/scep2019/results/ball_possession.txt")

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
