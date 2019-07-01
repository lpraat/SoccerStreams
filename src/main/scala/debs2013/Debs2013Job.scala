package debs2013

import java.util.Properties

import debs2013.operators.ball_possession.BallPossessionChecker
import debs2013.operators.shot_on_goal.ShotOnGoalChecker
import debs2013.operators.{EnrichedEventFlatMap, RawEventMap, UnusedDataFilter}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.runtime.state.filesystem.FsStateBackend
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
    override val Delay: Double =  3.092 + 0.9885
    override val Team: Int = 1
  }


  case object SecondHalf extends Half {
    override val Name: String = "second_half"
    override val StartTime: Long = 13086639146403495L
    override val Delay: Double = 0.455 + 0.84795
    override val Team: Int = 2
  }

  def build(half: Half, timestampFormat: TimestampFormat, startFromEarliest: Boolean, checkpointing: Boolean): Unit = {

    // Setup environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    if (checkpointing) {
      // Enables checkpointing on file, every 5 seconds.
      // Restore the topology in a maximum of 10 attempts with 10 seconds of delay between them
      env.enableCheckpointing(5000)
      env.setStateBackend(new FsStateBackend("file:///tmp/debs", false))
      env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 10000))
    }

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
      .addSource(kafkaSource).name("KafkaSource")
      .map(new RawEventMap()).name("RawEventMap")
      .filter(new UnusedDataFilter()).name("UnusedDataFilter")
      .flatMap(new EnrichedEventFlatMap()).name("EnrichedEventFlatMap")

    // Ball possession
    mainFLow
      .flatMap(new BallPossessionChecker(half, timestampFormat)).name("BallPossessionChecker").startNewChain()
      .writeAsText(f"/Users/lpraat/develop/scep2019/results/${half.Name}/ball_possession.txt", WriteMode.OVERWRITE)

    // Shots on goal
    mainFLow
      .flatMap(new ShotOnGoalChecker(half, timestampFormat)).startNewChain().name("ShotOnGoalChecker")
      .writeAsText(f"/Users/lpraat/develop/scep2019/results/${half.Name}/shots.txt", WriteMode.OVERWRITE)


    // Run topology
    env.execute()
  }

}
