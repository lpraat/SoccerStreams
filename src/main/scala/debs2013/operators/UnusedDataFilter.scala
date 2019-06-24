package debs2013.operators

import debs2013.Events.RawEvent
import org.apache.flink.api.common.functions.FilterFunction

class UnusedDataFilter extends FilterFunction[RawEvent] {
  override def filter(value: RawEvent): Boolean = {
    !(
    value.id == 105 || value.id == 106 || // referee
    value.id == 97 || value.id == 98   || // arms
    value.id == 99 || value.id == 100     // arms
    )
  }
}
