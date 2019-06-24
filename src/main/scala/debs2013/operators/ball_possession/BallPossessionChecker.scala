package debs2013.operators.ball_possession

import debs2013.Events.{EnrichedEvent}
import debs2013.Utils
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.util.Collector

import scala.collection.immutable.HashMap

class BallPossessionChecker extends RichFlatMapFunction[EnrichedEvent, String] with CheckpointedFunction {

  case class Possession(hits: Int, time: Long)
  case class LastHit(player: String, timestamp: Long)

  private var lastHit: LastHit = LastHit("", 0)
  private var lastHitState: ListState[LastHit] = _

  // TODO rename this variable
  private var playerToHit: HashMap[String, Possession] = _
  private var playerToHitState: ListState[HashMap[String, Possession]] = _

  override def flatMap(enrichedEvent: EnrichedEvent, out: Collector[String]): Unit = {
    val ballIsInTheField = Utils.isInTheField(enrichedEvent.ballEvent.x, enrichedEvent.ballEvent.y)
    val isGameInterrupted = enrichedEvent.gameEvent.interrupted

    if (ballIsInTheField && !isGameInterrupted) {
      val currPossession = playerToHit(enrichedEvent.player)

      if (checkHit(enrichedEvent)) {
        if (enrichedEvent.player != lastHit.player) {
          playerToHit = playerToHit.updated(enrichedEvent.player, currPossession.copy(hits = currPossession.hits + 1))
        } else {
          playerToHit = playerToHit.updated(enrichedEvent.player, currPossession.copy(time = currPossession.time + enrichedEvent.playerEvent.timestamp - lastHit.timestamp))
        }

        lastHit = LastHit(enrichedEvent.player, enrichedEvent.playerEvent.timestamp)

        // println(f"Touch at ${(enrichedEvent.playerEvent.timestamp - 10753295594424116L)*Math.pow(10, -12) + 3.092 + 0.9888} seconds")

        val c = (lastHit.timestamp - 10753295594424116L)*Math.pow(10, -12) + 3.092 + 0.9888

        out.collect(f"${c},${enrichedEvent.player},${playerToHit(enrichedEvent.player).time},${playerToHit(enrichedEvent.player).hits}")
      }
    } else {
      // Game is interrupted or ball is out of field so none is in possession of the ball right now
      lastHit = LastHit("", 0)
    }

  }

  def checkHit(enrichedEvent: EnrichedEvent): Boolean = {
    enrichedEvent.ballEvent.acc > 55 &&
    Utils.distance(
      enrichedEvent.playerEvent.x, enrichedEvent.playerEvent.y, enrichedEvent.playerEvent.z,
      enrichedEvent.ballEvent.x, enrichedEvent.ballEvent.y, enrichedEvent.ballEvent.z
    ) < 1
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    snapshotLastHitState()
    snapshotPlayerToHitState()
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    initializeLastHitState(context)
    initializePlayerToHitState(context)
  }

  def snapshotLastHitState(): Unit = {
    lastHitState.clear()
    lastHitState.add(lastHit)
  }

  def snapshotPlayerToHitState(): Unit = {
    playerToHitState.clear()
    playerToHitState.add(playerToHit)
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

  def initializePlayerToHitState(context: FunctionInitializationContext): Unit = {
    val descriptor = new ListStateDescriptor[HashMap[String, Possession]](
      "playerToHit",
      TypeInformation.of(new TypeHint[HashMap[String, Possession]]() {})
    )

    playerToHitState = context.getOperatorStateStore.getListState(descriptor)

    if (context.isRestored) {
      playerToHit = playerToHitState.get().iterator().next()
    } else {
      playerToHit = HashMap(
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
