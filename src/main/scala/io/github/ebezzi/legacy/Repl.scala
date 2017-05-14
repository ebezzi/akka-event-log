package io.github.ebezzi.legacy

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Tcp.{IncomingConnection, ServerBinding}
import akka.stream.scaladsl.{Flow, Framing, Source, Tcp}
import akka.util.ByteString

import scala.concurrent.Future

/**
  * Created by emanuele on 06/01/17.
  */
object ReplClient extends App {

  implicit val system = ActorSystem("tcp-manager")
  implicit val mat = ActorMaterializer()

  val connection = Tcp().outgoingConnection("127.0.0.1", 8888)

  val replParser =
    Flow[String].takeWhile(_ != "q")
      .concat(Source.single("BYE"))
      .map(elem => ByteString(s"$elem\n"))

  val repl = Flow[ByteString]
    .via(Framing.delimiter(
      ByteString("\n"),
      maximumFrameLength = 256,
      allowTruncation = true))
    .map(_.utf8String)
    .map(text => println("Server: " + text))
    .map(_ => readLine("> "))
    .via(replParser)

  connection.join(repl).run()

}

object ReplServer extends App {

  implicit val system = ActorSystem("tcp-manager")
  implicit val mat = ActorMaterializer()

  import akka.stream.scaladsl.Framing

  val connections: Source[IncomingConnection, Future[ServerBinding]] =
    Tcp().bind("localhost", 8888)
  connections runForeach { connection =>
    println(s"New connection from: ${connection.remoteAddress}")

    val echo = Flow[ByteString]
      .via(Framing.delimiter(
        ByteString("\n"),
        maximumFrameLength = 256,
        allowTruncation = true))
      .map(_.utf8String)
      .map(_ + "!!!\n")
      .map(ByteString(_))

    connection.handleWith(echo)
  }

}
