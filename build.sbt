name := "scep2019"
version := "0.1-SNAPSHOT"
organization := "org.example"
scalaVersion := "2.12.7"

val flinkVersion = "1.7.2"

libraryDependencies ++= Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion
)

libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.12"