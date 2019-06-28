package debs2013.operators.shot_on_goal

import debs2013.Debs2013Job.{Half, Standard, TimestampFormat}
import debs2013.Events.EnrichedEvent
import debs2013.Utils
import org.apache.flink.api.common.functions.{FlatMapFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper
import org.apache.flink.metrics.Meter
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.util.Collector

import scala.annotation.tailrec
import scala.collection.immutable.HashMap

class ShotOnGoalChecker(half: Half, timestampFormat: TimestampFormat) extends RichFlatMapFunction[EnrichedEvent, String] with CheckpointedFunction {
  private var playerToTeam: HashMap[String, Int] = _
  @transient private var playerToTeamState: ListState[HashMap[String, Int]] = _

  private var shotOnGoal: Boolean = false
  @transient private var shotOnGoalState: ListState[Boolean] = _

  private var shootingPlayer: String = ""
  @transient private var shootingPlayerState: ListState[String] = _

  private var lastUpdate: Long = 0
  @transient private var lastUpdateState: ListState[Long] = _

  @transient private var meter: Meter = _

  override def flatMap(enrichedEvent: EnrichedEvent, out: Collector[String]): Unit = {
    val ballIsInTheField = Utils.isInTheField(enrichedEvent.ballEvent.x, enrichedEvent.ballEvent.y)
    val isGameInterrupted = enrichedEvent.gameInterrupted

    if (ballIsInTheField && !isGameInterrupted) {

      if (shotOnGoal) {

        if (enrichedEvent.ballEvent.timestamp > lastUpdate) {

          // Here we should check if the player is the one who has initiated the shot.
          // But shots on goal can be deviated by another player
          // and we still want them to be attributed to the initial shooting player

          if (checkShotToGoal(enrichedEvent, getInGoalAreaFunction(shootingPlayer))) {

            if (timestampFormat == Standard) {
              out.collect(f"${enrichedEvent.ballEvent.timestamp},${shootingPlayer},${enrichedEvent.ballEvent.x},${enrichedEvent.ballEvent.y},${enrichedEvent.ballEvent.z},${enrichedEvent.ballEvent.vel},${enrichedEvent.ballEvent.velX},${enrichedEvent.ballEvent.velY},${enrichedEvent.ballEvent.velZ}, ${enrichedEvent.ballEvent.acc},${enrichedEvent.ballEvent.accX},${enrichedEvent.ballEvent.accY},${enrichedEvent.ballEvent.accZ}")
            } else {
              val oracleLikeTimestamp = Utils.getHourMinuteSeconds((enrichedEvent.ballEvent.timestamp - half.StartTime) * Math.pow(10, -12) + half.Delay)
              out.collect(f"${oracleLikeTimestamp},${shootingPlayer},${enrichedEvent.ballEvent.x},${enrichedEvent.ballEvent.y},${enrichedEvent.ballEvent.z},${enrichedEvent.ballEvent.vel},${enrichedEvent.ballEvent.velX},${enrichedEvent.ballEvent.velY},${enrichedEvent.ballEvent.velZ}, ${enrichedEvent.ballEvent.acc},${enrichedEvent.ballEvent.accX},${enrichedEvent.ballEvent.accY},${enrichedEvent.ballEvent.accZ}")
            }

            lastUpdate = enrichedEvent.ballEvent.timestamp

          } else {
            shotOnGoal = false
            shootingPlayer = ""
          }
        }

      } else if (!shotOnGoal && checkHit(enrichedEvent)) {

          if (checkShotToGoal(enrichedEvent, getInGoalAreaFunction(enrichedEvent.player))) {
            shotOnGoal = true
            shootingPlayer = enrichedEvent.player

            if (timestampFormat == Standard) {
              out.collect(f"${enrichedEvent.ballEvent.timestamp},${shootingPlayer},${enrichedEvent.ballEvent.x},${enrichedEvent.ballEvent.y},${enrichedEvent.ballEvent.z},${enrichedEvent.ballEvent.vel},${enrichedEvent.ballEvent.velX},${enrichedEvent.ballEvent.velY},${enrichedEvent.ballEvent.velZ}, ${enrichedEvent.ballEvent.acc},${enrichedEvent.ballEvent.accX},${enrichedEvent.ballEvent.accY},${enrichedEvent.ballEvent.accZ}")
            } else {
              val oracleLikeTimestamp = Utils.getHourMinuteSeconds((enrichedEvent.ballEvent.timestamp - half.StartTime)*Math.pow(10, -12) + half.Delay)
              out.collect(f"${oracleLikeTimestamp},${shootingPlayer},${enrichedEvent.ballEvent.x},${enrichedEvent.ballEvent.y},${enrichedEvent.ballEvent.z},${enrichedEvent.ballEvent.vel},${enrichedEvent.ballEvent.velX},${enrichedEvent.ballEvent.velY},${enrichedEvent.ballEvent.velZ}, ${enrichedEvent.ballEvent.acc},${enrichedEvent.ballEvent.accX},${enrichedEvent.ballEvent.accY},${enrichedEvent.ballEvent.accZ}")
            }

            lastUpdate = enrichedEvent.ballEvent.timestamp
          }
      }

    } else {
      if (shotOnGoal) {
        shotOnGoal = false
        shootingPlayer = ""
      }
    }

    meter.markEvent()
  }

  def getInGoalAreaFunction(player: String): (Float, Float, Float) => Boolean = {
    if (playerToTeam(player) == half.Team) {
      Utils.inGoalAreaOfTeam1
    } else {
      Utils.inGoalAreaOfTeam2
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

/*    val ax: Float = enrichedEvent.ballEvent.acc * enrichedEvent.ballEvent.accX
    val ay: Float = enrichedEvent.ballEvent.acc * enrichedEvent.ballEvent.accY
    val az: Float = enrichedEvent.ballEvent.acc * enrichedEvent.ballEvent.accZ - 9.8f*/

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


  override def open(parameters: Configuration): Unit = {
    this.meter = getRuntimeContext
      .getMetricGroup
      .meter("AverageThroughput", new DropwizardMeterWrapper(new com.codahale.metrics.Meter()))
  }


  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    snapshotPlayerToTeamState()
    snapshotShotOnGoalState()
    snapshotShootingPlayerState()
    snapshotLastUpdateState()
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    initializePlayerToTeamState(context)
    initializeShotOnGoalState(context)
    initializeShootingPlayerState(context)
    initializeLastUpdateState(context)
  }

  def snapshotLastUpdateState(): Unit = {
    lastUpdateState.clear()
    lastUpdateState.add(lastUpdate)
  }

  def snapshotPlayerToTeamState(): Unit = {
    playerToTeamState.clear()
    playerToTeamState.add(playerToTeam)
  }

  def snapshotShotOnGoalState(): Unit = {
    shotOnGoalState.clear()
    shotOnGoalState.add(shotOnGoal)
  }

  def snapshotShootingPlayerState(): Unit = {
    shootingPlayerState.clear()
    shootingPlayerState.add(shootingPlayer)
  }

  def initializePlayerToTeamState(context: FunctionInitializationContext): Unit = {
    val descriptor = new ListStateDescriptor[HashMap[String, Int]](
      "playerToTeam",
      TypeInformation.of(new TypeHint[HashMap[String, Int]]() {})
    )

    playerToTeamState = context.getOperatorStateStore.getListState(descriptor)

    if (context.isRestored) {
      playerToTeam = playerToTeamState.get().iterator().next()
    } else {
      playerToTeam = HashMap(
        "Nick Gertje" -> 1,
        "Dennis Dotterweich" -> 1,
        "Niklas Waelzlein" -> 1,
        "Wili Sommer" -> 1,
        "Philipp Harlass" -> 1,
        "Roman Hartleb" -> 1,
        "Erik Engelhardt" -> 1,
        "Sandro Schneider" -> 1,

        "Leon Krapf" -> 2,
        "Kevin Baer" -> 2,
        "Luca Ziegler" -> 2,
        "Ben Mueller" -> 2,
        "Vale Reitstetter" -> 2,
        "Christopher Lee" -> 2,
        "Leon Heinze" -> 2,
        "Leo Langhans" -> 2
      )
    }
  }

  def initializeShotOnGoalState(context: FunctionInitializationContext): Unit = {
    val descriptor = new ListStateDescriptor[Boolean](
      "shotOnGoal",
      TypeInformation.of(new TypeHint[Boolean]() {})
    )

    shotOnGoalState = context.getOperatorStateStore.getListState(descriptor)

    if (context.isRestored) {
      shotOnGoal = shotOnGoalState.get().iterator().next()
    }
  }

  def initializeShootingPlayerState(context: FunctionInitializationContext): Unit = {
    val descriptor = new ListStateDescriptor[Boolean](
      "shootingPlayer",
      TypeInformation.of(new TypeHint[Boolean]() {})
    )

    shotOnGoalState = context.getOperatorStateStore.getListState(descriptor)

    if (context.isRestored) {
      shotOnGoal = shotOnGoalState.get().iterator().next()
    }
  }


  def initializeLastUpdateState(context: FunctionInitializationContext): Unit = {
    val descriptor = new ListStateDescriptor[Long](
      "lastUpdate",
      TypeInformation.of(new TypeHint[Long]() {})
    )

    lastUpdateState = context.getOperatorStateStore.getListState(descriptor)

    if (context.isRestored) {
      lastUpdate = lastUpdateState.get().iterator().next()
    }
  }

}
