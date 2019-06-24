package debs2013.operators

import debs2013.Events.RawEvent
import org.apache.flink.api.common.functions.MapFunction

class RawEventMap extends MapFunction[String, RawEvent] {

  override def map(eventStr: String): RawEvent = {
    val eventStrFields: Array[String] = eventStr.split(",")
    RawEvent(
      eventStrFields(0).toLong, // event id
      eventStrFields(1).toLong, // timestamp
      eventStrFields(2).toFloat * 1e-3f, // x in m
      eventStrFields(3).toFloat * 1e-3f, // y in m
      Math.max(eventStrFields(4).toFloat * 1e-3f, 0), // z in m
      eventStrFields(5).toFloat * 1e-6f, // |v| in m/s
      eventStrFields(6).toFloat * 1e-6f, // |a| in m/s^2
      eventStrFields(7).toFloat * 1e-4f, // vx
      eventStrFields(8).toFloat * 1e-4f, // vy
      eventStrFields(9).toFloat * 1e-4f, // vz
      eventStrFields(10).toFloat * 1e-4f, // ax
      eventStrFields(11).toFloat * 1e-4f, // ay
      eventStrFields(12).toFloat * 1e-4f // az
    )
  }

}