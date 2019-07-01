package debs2013

import debs2013.Debs2013Job.{FirstHalf, OracleLike, Standard}


object Main extends App {

  // TODO add some docs

  Debs2013Job.build(
    half = FirstHalf,
    timestampFormat = Standard,
    startFromEarliest = true,
    checkpointing = false
  )

}
