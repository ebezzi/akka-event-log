package io.github.ebezzi.streams

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Sink, Source}

object StreamTests extends App {

  implicit val system = ActorSystem()
  import system.dispatcher
  implicit val mat = ActorMaterializer()

  val x = Source.queue[String](5, OverflowStrategy.fail)
    .map { x => println(x); x }
    .to(Sink.ignore)
    .run()

  x.offer("stocazzo")

}
