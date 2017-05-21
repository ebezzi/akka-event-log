name := """akka-distributed-log"""

version := "1.0"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.5.1",
  "com.typesafe.akka" %% "akka-stream" % "2.5.1",
  "com.typesafe.akka" %% "akka-remote" % "2.5.1",
  "com.typesafe.akka" %% "akka-cluster" % "2.5.1",
  "com.typesafe.akka" %% "akka-testkit" % "2.5.1" % "test",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test")
