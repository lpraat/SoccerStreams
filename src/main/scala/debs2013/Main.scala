package debs2013

import debs2013.Debs2013Job.{FirstHalf, OracleLike}


object Main extends App {

  // TODO add some docs

  Debs2013Job.build(
    half = FirstHalf,
    timestampFormat = OracleLike,
    startFromEarliest = false,
    checkpointing = true
  )

}
