package debs2013

object Events {
  
  case class SensorEvent(id: Long, timestamp: Long,
                         x: Long, y: Long, z: Long,
                         vel: Long, acc: Long)

}
