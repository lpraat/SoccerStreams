package org.example

import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.createLocalEnvironment()

    val text = env.fromElements("cr cr cr cr cr cr cr")
    val counts = text.flatMap { _.toLowerCase.split("\\W+") }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    counts.print()

  }
}
