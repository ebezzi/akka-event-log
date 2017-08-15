package io.github.ebezzi.streams

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Tcp.{IncomingConnection, ServerBinding}
import akka.stream.scaladsl.{Flow, Sink, Source, Tcp}
import akka.util.ByteString

import scala.concurrent.Future

/**
  * Created by ebezzi on 15/07/2017.
  */
object StreamServer extends App {

  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()

  import akka.stream.scaladsl.Framing

  val host = "localhost"
  val port = 8000

  val connections: Source[IncomingConnection, Future[ServerBinding]] =
    Tcp().bind(host, port)

  connections runForeach { connection =>
    println(s"New connection from: ${connection.remoteAddress}")

    val echo = Flow[ByteString]
      .via(Framing.delimiter(
        ByteString("\n"),
        maximumFrameLength = 256,
        allowTruncation = true))
      .map(_.utf8String)
      .map { x => println(s"received $x"); x}
      .map(_ + "!!!\n")
      .map(ByteString(_))

    connection.handleWith(echo)
//
//    val commandParser = Flow[String].takeWhile(_ != "BYE").map(_ + "!")
//
//    import connection._
//    val welcomeMsg = s"Welcome to: $localAddress, you are: $remoteAddress!"
//    val welcome = Source.single(welcomeMsg)
//
//    val serverLogic = Flow[ByteString]
//      .via(Framing.delimiter(
//        ByteString("\n"),
//        maximumFrameLength = 256,
//        allowTruncation = true))
//      .map(_.utf8String)
//      .via(commandParser)
//      // merge in the initial banner after parser
//      .merge(welcome)
//      .map(_ + "\n")
//      .map(ByteString(_))
//
//    connection.handleWith(serverLogic)
//
//
  }
}
