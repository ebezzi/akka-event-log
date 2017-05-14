package io.github.ebezzi.legacy

import java.io.File

import akka.actor.ActorSystem
import akka.stream.scaladsl.Tcp.{IncomingConnection, ServerBinding}
import akka.stream.scaladsl.{Flow, Source, Tcp}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import akka.util.ByteString
import io.github.ebezzi.LogReader

import scala.concurrent.Future


class Server {

  var lastCommittedOffset = 0L

  val file = new File("00000.dat")
  val reader = new LogReader(file)

  val host = "localhost"
  val port = 7777

  implicit val system = ActorSystem("tcp-manager")

  val decider: Supervision.Decider = { e =>
    e.printStackTrace()
    Supervision.Stop
  }

  val materializerSettings = ActorMaterializerSettings(system).withSupervisionStrategy(decider)
  implicit val materializer = ActorMaterializer(materializerSettings)(system)
  import akka.stream.scaladsl.Framing

  val connections: Source[IncomingConnection, Future[ServerBinding]] =
    Tcp().bind(host, port)

  connections.runForeach { connection =>
    println(s"New connection from: ${connection.remoteAddress}")

    val newLineFraming = Framing.delimiter(
      ByteString("\n"),
      maximumFrameLength = 256,
      allowTruncation = true)

    val lengthByteFraming = Framing.lengthField(4, maximumFrameLength = Int.MaxValue)

    val echo = Flow[ByteString]
      .via(lengthByteFraming) // read the last committed offset
      .map(_.drop(4).asByteBuffer.getInt) // drop the length byte
//      .map(_.decodeString("utf-8").trim.toInt)
      .map { offset =>
        lastCommittedOffset = offset
        reader.moveTo(1 + lastCommittedOffset)
        reader.next()
      }
      .map(ByteString(_)) //
      .map { x => println(x.decodeString("utf-8")); x }

    connection.handleWith(echo)
  }

}

object Server extends App {

  val server = new Server

}
