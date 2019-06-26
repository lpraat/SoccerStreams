package debs2013

import java.util.Properties

import debs2013.operators.ball_possession.BallPossessionChecker
import debs2013.operators.shot_on_goal.ShotOnGoalChecker
import debs2013.operators.{EnrichedEventMap, RawEventMap, UnusedDataFilter}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object Debs2013Job {
  sealed trait TimestampFormat
  case object OracleLike extends TimestampFormat
  case object Standard extends TimestampFormat

  sealed trait Half {
    val Name: String
    val StartTime: Long
    val Delay: Double
    val Team: Int
  }

  case object FirstHalf extends Half {
    override val Name: String = "first_half"
    override val StartTime: Long = 10753295594424116L
    override val Delay: Double =  3.092 + 0.9888
    override val Team: Int = 1
  }


  case object SecondHalf extends Half {
    override val Name: String = "second_half"
    override val StartTime: Long = 13086639146403495L
    override val Delay: Double = 0.455 + 0.8482
    override val Team: Int = 2
  }

  def build(half: Half, timestampFormat: TimestampFormat, startFromEarliest: Boolean): Unit = {

    // Setup environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // Setup source
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")

    val topicName = if (startFromEarliest) f"${half.Name}_full" else half.Name
    val kafkaSource = new FlinkKafkaConsumer[String](topicName, new SimpleStringSchema(), properties)

    kafkaSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor[String] {
      def extractAscendingTimestamp(eventStr: String): Long = {
        eventStr.split(",")(1).toLong
      }
    })

    if (startFromEarliest) {
      kafkaSource.setStartFromEarliest()
    }

    // Create topology
    val mainFLow =   env
      .addSource(kafkaSource)
      .map(new RawEventMap())
      .filter(new UnusedDataFilter())
      .flatMap(new EnrichedEventMap())

    // Ball possession
    mainFLow
      .flatMap(new BallPossessionChecker(half, timestampFormat)).startNewChain()
      .writeAsText(f"/Users/lpraat/develop/scep2019/results/${half.Name}/ball_possession.txt")

    // Shots on goal
    mainFLow
      .flatMap(new ShotOnGoalChecker(half, timestampFormat)).startNewChain()
      .writeAsText(f"/Users/lpraat/develop/scep2019/results/${half.Name}/shots.txt")


    // Run topology
    env.execute()
  }

}
