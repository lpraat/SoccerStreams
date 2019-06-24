package debs2013.operators

import debs2013.Events.{BallPossessionEvent, EnrichedEvent}
import debs2013.Utils
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.util.Collector

class BallPossessionChecker extends RichFlatMapFunction[EnrichedEvent, BallPossessionEvent] with CheckpointedFunction {

  private var playerWithBall: String = ""
  private var playerWithBallState: ListState[String] = _

  override def flatMap(enrichedEvent: EnrichedEvent, out: Collector[BallPossessionEvent]): Unit = {
    if (checkHit(enrichedEvent)) {
      println(enrichedEvent.player)
      println(f"Touch at ${(enrichedEvent.playerEvent.timestamp - 10753295594424116L)*Math.pow(10, -12) + 3.092 + 0.9888} seconds")
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
    snapshotPlayerWithBallState()
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    initializePlayerWithBallState(context)
  }

  def snapshotPlayerWithBallState(): Unit = {
    playerWithBallState.clear()
    playerWithBallState.add(playerWithBall)
  }

  def initializePlayerWithBallState(context: FunctionInitializationContext): Unit = {
    val descriptor = new ListStateDescriptor[String](
      "playerWithBall",
      TypeInformation.of(new TypeHint[String]() {})
    )

    playerWithBallState = context.getOperatorStateStore.getListState(descriptor)

    if (context.isRestored) {
      playerWithBall = playerWithBallState.get().iterator().next()
    }
  }

}
