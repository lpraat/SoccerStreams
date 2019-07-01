package debs2013.operators.shot_on_goal

import debs2013.Debs2013Job.{Half, Standard, TimestampFormat}
import debs2013.Events.EnrichedEvent
import debs2013.Utils
import org.apache.flink.api.common.functions.{FlatMapFunction, RichFlatMapFunction}
import org.apache.flink.util.Collector

import scala.annotation.tailrec

class ShotOnGoalChecker extends FlatMapFunction[EnrichedEvent, (EnrichedEvent, Boolean, Boolean, Boolean)] {

  override def flatMap(enrichedEvent: EnrichedEvent, out: Collector[(EnrichedEvent, Boolean, Boolean, Boolean)]): Unit = {
    val gameOff = !Utils.isInTheField(enrichedEvent.ballEvent.x, enrichedEvent.ballEvent.y) || enrichedEvent.gameInterrupted

    if (gameOff) {
      out.collect(enrichedEvent.copy(gameInterrupted = gameOff), false, false, false)
    } else {
      out.collect(enrichedEvent.copy(gameInterrupted = gameOff), checkHit(enrichedEvent), checkShotToGoal(enrichedEvent, Utils.inGoalAreaOfTeam1), checkShotToGoal(enrichedEvent, Utils.inGoalAreaOfTeam2))
    }

  }

  def checkHit(enrichedEvent: EnrichedEvent): Boolean = {
    enrichedEvent.ballEvent.acc > 55 &&
      Utils.distance(
        enrichedEvent.playerEvent.x, enrichedEvent.playerEvent.y, enrichedEvent.playerEvent.z,
        enrichedEvent.ballEvent.x, enrichedEvent.ballEvent.y, enrichedEvent.ballEvent.z
      ) < 1
  }

  def checkShotToGoal(enrichedEvent: EnrichedEvent, inGoalArea: (Float, Float, Float) => Boolean): Boolean = {
    val x0: Float = enrichedEvent.ballEvent.x
    val y0: Float = enrichedEvent.ballEvent.y
    val z0: Float = enrichedEvent.ballEvent.z

    val v0x: Float = enrichedEvent.ballEvent.vel * enrichedEvent.ballEvent.velX
    val v0y: Float = enrichedEvent.ballEvent.vel * enrichedEvent.ballEvent.velY
    val v0z: Float = enrichedEvent.ballEvent.vel * enrichedEvent.ballEvent.velZ

    @tailrec
    def loop(t: Float): Boolean = {
      if (t > 1.6) {
        false
      } else {
        if (inGoalArea(x0 + v0x*t , y0 + v0y*t, Math.max(z0 + v0z*t, 0))) {
          true
        } else {
          loop(t + 0.01f)
        }
      }
    }

    loop(0f)
  }

}
