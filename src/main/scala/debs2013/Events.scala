package debs2013

object Events {
  
  case class RawEvent(id: Long, timestamp: Long,
                      x: Float, y: Float, z: Float,
                      vel: Float, acc: Float,
                      velX: Float, velY: Float, velZ: Float,
                      accX: Float, accY: Float, accZ: Float) {

    def toBallEvent: BallEvent = {
      BallEvent(id, timestamp, x, y, z, vel, acc, velX, velY, velZ, accX, accY, accZ)
    }

    def toPlayerEvent: PlayerEvent = {
      PlayerEvent(id, timestamp, x, y, z, vel, acc, velX, velY, velZ, accX, accY, accZ)
    }
  }

  case class BallEvent(id: Long, timestamp: Long,
                       x: Float, y: Float, z: Float,
                       vel: Float, acc: Float,
                       velX: Float, velY: Float, velZ: Float,
                       accX: Float, accY: Float, accZ: Float)

  case class PlayerEvent(id: Long, timestamp: Long,
                         x: Float, y: Float, z: Float,
                         vel: Float, acc: Float,
                         velX: Float, velY: Float, velZ: Float,
                         accX: Float, accY: Float, accZ: Float)

  case class GameEvent(id: Long, timestamp: Long, interrupted: Boolean)


  case class EnrichedEvent(player: String, kind: String,
                           playerEvent: PlayerEvent,
                           ballEvent: BallEvent,
                           gameEvent: GameEvent)

  case class BallPossessionEvent(timestamp: Double, player: String, time: Long, hits: Int)

}
