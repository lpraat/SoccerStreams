package debs2013.operators.ball_possession

import debs2013.Debs2013Job.{Half, Standard, TimestampFormat}
import debs2013.Events.EnrichedEvent
import debs2013.Utils
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper
import org.apache.flink.metrics.Meter
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, RichAllWindowFunction, RichWindowFunction}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.immutable.HashMap

case class Possession(hits: Int, time: Long)
case class LastHit(player: String, timestamp: Long)

class BallPossessionMerge(half: Half, timestampFormat: TimestampFormat) extends RichAllWindowFunction[(EnrichedEvent, Boolean), String, TimeWindow] with CheckpointedFunction {
  private var lastHit: LastHit = LastHit("", 0)
  @transient private var lastHitState: ListState[LastHit] = _

  @transient private var playerToPossession: HashMap[String, Possession] = _
  @transient private var playerToPossessionState: ListState[HashMap[String, Possession]] = _

  @transient private var meter: Meter = _

  override def apply(window: TimeWindow, input: Iterable[(EnrichedEvent, Boolean)], out: Collector[String]): Unit = {
    val windowData = input.toArray.sortBy(el => el._1.playerEvent.timestamp)

    windowData.foreach(el =>  {
      val (enrichedEvent, isHit) = el

      if (!enrichedEvent.gameInterrupted) {
        val currPossession = playerToPossession(enrichedEvent.player)

        if (isHit) {
          if (enrichedEvent.player != lastHit.player) {
            playerToPossession = playerToPossession.updated(enrichedEvent.player, currPossession.copy(hits = currPossession.hits + 1))
          } else {
            playerToPossession = playerToPossession.updated(enrichedEvent.player, currPossession.copy(time = currPossession.time + enrichedEvent.playerEvent.timestamp - lastHit.timestamp))
          }

          lastHit = LastHit(enrichedEvent.player, enrichedEvent.playerEvent.timestamp)

          if (timestampFormat == Standard) {
            out.collect(f"${enrichedEvent.playerEvent.timestamp},${enrichedEvent.player},${playerToPossession(enrichedEvent.player).time},${playerToPossession(enrichedEvent.player).hits}")
          } else {
            val oracleLikeTimestamp = Utils.getHourMinuteSeconds((lastHit.timestamp - half.StartTime)*Math.pow(10, -12) + half.Delay)
            out.collect(f"${oracleLikeTimestamp},${enrichedEvent.player},${playerToPossession(enrichedEvent.player).time},${playerToPossession(enrichedEvent.player).hits}")
          }
        }
      } else {
        // Game is interrupted or ball is out of field so none is in possession of the ball right now
        lastHit = LastHit("", 0)
      }

      meter.markEvent()
    })
  }

  override def open(parameters: Configuration): Unit = {
    this.meter = getRuntimeContext
      .getMetricGroup
      .meter("AverageThroughput", new DropwizardMeterWrapper(new com.codahale.metrics.Meter()))
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    snapshotLastHitState()
    snapshotPlayerToPossessionState()
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    initializeLastHitState(context)
    initializePlayerToPossessionState(context)
  }

  def snapshotLastHitState(): Unit = {
    lastHitState.clear()
    lastHitState.add(lastHit)
  }

  def snapshotPlayerToPossessionState(): Unit = {
    playerToPossessionState.clear()
    playerToPossessionState.add(playerToPossession)
  }

  def initializeLastHitState(context: FunctionInitializationContext): Unit = {
    val descriptor = new ListStateDescriptor[LastHit](
      "lastHit",
      TypeInformation.of(new TypeHint[LastHit]() {})
    )

    lastHitState = context.getOperatorStateStore.getListState(descriptor)

    if (context.isRestored) {
      lastHit = lastHitState.get().iterator().next()
    }
  }

  def initializePlayerToPossessionState(context: FunctionInitializationContext): Unit = {
    val descriptor = new ListStateDescriptor[HashMap[String, Possession]](
      "playerToPossession",
      TypeInformation.of(new TypeHint[HashMap[String, Possession]]() {})
    )

    playerToPossessionState = context.getOperatorStateStore.getListState(descriptor)

    if (context.isRestored) {
      playerToPossession = playerToPossessionState.get().iterator().next()
    } else {
      playerToPossession = HashMap(
        "Nick Gertje" -> Possession(0, 0),
        "Dennis Dotterweich" -> Possession(0, 0),
        "Niklas Waelzlein" -> Possession(0, 0),
        "Wili Sommer" -> Possession(0, 0),
        "Philipp Harlass" -> Possession(0, 0),
        "Roman Hartleb" -> Possession(0, 0),
        "Erik Engelhardt" -> Possession(0, 0),
        "Sandro Schneider" -> Possession(0, 0),

        "Leon Krapf" -> Possession(0, 0),
        "Kevin Baer" -> Possession(0, 0),
        "Luca Ziegler" -> Possession(0, 0),
        "Ben Mueller" -> Possession(0, 0),
        "Vale Reitstetter" -> Possession(0, 0),
        "Christopher Lee" -> Possession(0, 0),
        "Leon Heinze" -> Possession(0, 0),
        "Leo Langhans" -> Possession(0, 0)
      )
    }
  }
}
