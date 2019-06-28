package debs2013.operators

import debs2013.Events._
import debs2013.Utils
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.util.Collector

import scala.collection.immutable.HashMap

class EnrichedEventMap extends RichFlatMapFunction[RawEvent, EnrichedEvent] with CheckpointedFunction {
  private var gameInterrupted: Boolean = false
  @transient private var gameInterruptedState: ListState[Boolean] = _

  private var ball: BallEvent = _
  @transient private var ballState: ListState[BallEvent] = _

  private var pending: Vector[PlayerEvent] = Vector()
  @transient private var pendingState: ListState[Vector[PlayerEvent]] = _

  @transient private var sensorPlayerMapping: HashMap[Long, (String, String)] = _

  override def flatMap(rawEvent: RawEvent, collector: Collector[EnrichedEvent]): Unit = {
    if (Utils.isGameInterruption(rawEvent.id)) {
      if (rawEvent.id == 1) {
        gameInterrupted = true
      } else {
        gameInterrupted = false
      }
    } else if (Utils.isBall(rawEvent.id)) {
      if (Utils.isInTheField(rawEvent.x, rawEvent.y)) {
        ball = rawEvent.toBallEvent
        if (pending.nonEmpty) {
          pending.foreach(playerEvent => collector.collect(enrichRawEvent(playerEvent)))
          pending = Vector()
        }
      }
    } else {
      pending = pending :+ rawEvent.toPlayerEvent
    }
  }

  def enrichRawEvent(playerEvent: PlayerEvent): EnrichedEvent = {
    val id = playerEvent.id
    val (player, kind) = sensorPlayerMapping(id)

    EnrichedEvent(
      player, kind,
      playerEvent,
      ball,
      gameInterrupted
    )
  }

  def createSensorPlayerMapping(): HashMap[Long, (String, String)] = {

    //  Nick Gertje (Left Leg: 13, Right Leg: 14, Left Arm: 97, Right Arm: 98)
    //
    //  Dennis Dotterweich (Left Leg: 47, Right Leg:16)
    //
    //  Niklas Waelzlein (Left Leg: 49, Right Leg: 88)
    //
    //  Wili Sommer (Left Leg: 19, Right Leg: 52)
    //
    //  Philipp Harlass (Left Leg: 53, Right Leg: 54)
    //
    //  Roman Hartleb (Left Leg: 23, Right Leg: 24)
    //
    //  Erik Engelhardt (Left Leg: 57, Right Leg: 58)
    //
    //  Sandro Schneider (Left Leg: 59, Right Leg: 28)

    HashMap(
      13L -> ("Nick Gertje", "Left"),
      14L -> ("Nick Gertje", "Right"),

      47L -> ("Dennis Dotterweich", "Left"),
      16L -> ("Dennis Dotterweich", "Right"),

      49L -> ("Niklas Waelzlein", "Left"),
      88L -> ("Niklas Waelzlein", "Right"),

      19L -> ("Wili Sommer", "Left"),
      52L -> ("Wili Sommer", "Right"),

      53L -> ("Philipp Harlass", "Left"),
      54L -> ("Philipp Harlass", "Right"),

      23L -> ("Roman Hartleb", "Left"),
      24L -> ("Roman Hartleb", "Right"),

      57L -> ("Erik Engelhardt", "Left"),
      58L -> ("Erik Engelhardt", "Right"),

      59L -> ("Sandro Schneider", "Left"),
      28L -> ("Sandro Schneider", "Right"),

      //      Leon Krapf (Left Leg: 61, Right Leg: 62, Left Arm: 99, Right Arm: 100)
      //
      //      Kevin Baer (Left Leg: 63, Right Leg: 64)
      //
      //      Luca Ziegler (Left Leg: 65, Right Leg: 66)
      //
      //      Ben Mueller (Left Leg: 67, Right Leg: 68)
      //
      //      Vale Reitstetter (Left Leg: 69, Right Leg: 38)
      //
      //      Christopher Lee (Left Leg: 71, Right Leg: 40)
      //
      //      Leon Heinze (Left Leg: 73, Right Leg: 74)
      //
      //      Leo Langhans (Left Leg: 75, Right Leg: 44)

      61L -> ("Leon Krapf", "Left"),
      62L -> ("Leon Krapf", "Right"),

      63L -> ("Kevin Baer", "Left"),
      64L -> ("Kevin Baer", "Right"),

      65L -> ("Luca Ziegler", "Left"),
      66L -> ("Luca Ziegler", "Right"),

      67L -> ("Ben Mueller", "Left"),
      68L -> ("Ben Mueller", "Right"),

      69L -> ("Vale Reitstetter", "Left"),
      38L -> ("Vale Reitstetter", "Right"),

      71L -> ("Christopher Lee", "Left"),
      40L -> ("Christopher Lee", "Right"),

      73L -> ("Leon Heinze", "Left"),
      74L -> ("Leon Heinze", "Right"),

      75L -> ("Leo Langhans", "Left"),
      44L -> ("Leo Langhans", "Right")
    )
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    snapshotBallState()
    snapshotGameInterruptedState()
    snapshotPendingState()
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    sensorPlayerMapping = createSensorPlayerMapping()

    initializeBallState(context)
    initializeGameInterruptedState(context)
    initializePendingState(context)
  }

  def initializeBallState(context: FunctionInitializationContext): Unit = {
    val descriptor = new ListStateDescriptor[BallEvent](
      "ball",
      TypeInformation.of(new TypeHint[BallEvent]() {})
    )

    ballState = context.getOperatorStateStore.getListState(descriptor)

    if (context.isRestored) {
      ball = ballState.get().iterator().next()
    }
  }

  def initializeGameInterruptedState(context: FunctionInitializationContext): Unit = {
    val descriptor = new ListStateDescriptor[Boolean](
      "gameInterrupted",
      TypeInformation.of(new TypeHint[Boolean]() {})
    )

    gameInterruptedState = context.getOperatorStateStore.getListState(descriptor)

    if (context.isRestored) {
      gameInterrupted = gameInterruptedState.get().iterator().next()
    }
  }

  def initializePendingState(context: FunctionInitializationContext): Unit = {
    val descriptor = new ListStateDescriptor[Vector[PlayerEvent]](
      "pendingState",
      TypeInformation.of(new TypeHint[Vector[PlayerEvent]]() {})
    )

    pendingState = context.getOperatorStateStore.getListState(descriptor)

    if (context.isRestored) {
      pending = pendingState.get().iterator().next()
    }
  }

  def snapshotBallState(): Unit = {
    ballState.clear()
    ballState.add(ball)
  }

  def snapshotGameInterruptedState(): Unit = {
    gameInterruptedState.clear()
    gameInterruptedState.add(gameInterrupted)
  }

  def snapshotPendingState(): Unit = {
    pendingState.clear()
    pendingState.add(pending)
  }
}
