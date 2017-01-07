package io.github.ebezzi

import akka.actor.{ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import akka.stream.scaladsl.Tcp._
import akka.util.ByteString

import scala.concurrent.Future

class StreamingTcpManager {

}

object StreamingTcpManager extends App {

  val host = "localhost"
  val port = 7777

  implicit val system = ActorSystem("tcp-manager")
  implicit val mat = ActorMaterializer()

  import akka.stream.scaladsl.Framing

  val connections: Source[IncomingConnection, Future[ServerBinding]] =
    Tcp().bind(host, port)

  connections.runForeach { connection =>
    println(s"New connection from: ${connection.remoteAddress}")

    val framing = Framing.delimiter(
      ByteString("\n"),
      maximumFrameLength = 256,
      allowTruncation = true)

//    val framing = Framing.lengthField(4, maximumFrameLength = Int.MaxValue)

    val echo = Flow[ByteString]
      .via(framing)
      .map(_.utf8String)
      .map(_ + "!!!\n")
      .map{x => println(x); x}
      .map(ByteString(_))

    connection.handleWith(echo)
  }

}