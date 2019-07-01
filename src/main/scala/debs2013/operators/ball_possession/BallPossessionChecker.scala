package debs2013.operators.ball_possession

import debs2013.Events.EnrichedEvent
import debs2013.Utils
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

class BallPossessionChecker extends FlatMapFunction[EnrichedEvent, (EnrichedEvent, Boolean)] {
  override def flatMap(enrichedEvent: EnrichedEvent, out: Collector[(EnrichedEvent, Boolean)]): Unit = {
    val gameOff = !Utils.isInTheField(enrichedEvent.ballEvent.x, enrichedEvent.ballEvent.y) || enrichedEvent.gameInterrupted

    if (gameOff) {
      out.collect(enrichedEvent.copy(gameInterrupted = gameOff), false)
    } else {
      if (checkHit(enrichedEvent)) {
        out.collect(enrichedEvent.copy(gameInterrupted = gameOff), true)
      } else {
        out.collect(enrichedEvent.copy(gameInterrupted = gameOff), false)
      }
    }
  }

  def checkHit(enrichedEvent: EnrichedEvent): Boolean = {
    enrichedEvent.ballEvent.acc > 55 &&
    Utils.distance(
      enrichedEvent.playerEvent.x, enrichedEvent.playerEvent.y, enrichedEvent.playerEvent.z,
      enrichedEvent.ballEvent.x, enrichedEvent.ballEvent.y, enrichedEvent.ballEvent.z
    ) < 1
  }

}
