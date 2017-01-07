package io.github.ebezzi

import java.io.File
import java.nio.ByteOrder

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import akka.stream.scaladsl.{Flow, Framing, Sink, Source, Tcp}
import akka.util.{ByteString, ByteStringBuilder}

import scala.concurrent.Future

/**
  * Created by emanuele on 31/12/16.
  */
object Consumer extends App {

  new Consumer

}

class Consumer {

  implicit val system = ActorSystem("consumer")
  val decider: Supervision.Decider = { e =>
    e.printStackTrace()
    Supervision.Stop
  }

  val materializerSettings = ActorMaterializerSettings(system).withSupervisionStrategy(decider)
  implicit val materializer = ActorMaterializer(materializerSettings)(system)

  import system.dispatcher

  val connection = Tcp().outgoingConnection("127.0.0.1", 7777)

  implicit val byteOrder = ByteOrder.LITTLE_ENDIAN

  def encode(ba: Array[Byte]) =
    new ByteStringBuilder()
      .putInt(ba.length)
      .putBytes(ba)
      .result()

  def send(ba: Array[Byte]): Future[ByteString] =
    Source.single(ba)
      .map(encode)
      .via(connection)
      .runWith(Sink.head)

  //  val initial = Source.single()

//  val flow = Flow[ByteString]
//    //    .via(Framing.lengthField(4, maximumFrameLength = Int.MaxValue))
//    .map { x => println(x); x } // prints the offset
//    .map(_.drop(4))
//    .map { x => println(x.decodeString("utf-8")); x }
//    .merge(Source.single(encode(Array[Byte](0, 0, 0, 0)))) // prints the offset
//
//
//  connection.join(flow).run()
//    .foreach { e =>
//    send(Array[Byte](0, 0, 0, 0))
//  }

  send(Array[Byte](0, 0, 0, 0)).foreach(println)
//  send(Array[Byte](0, 0, 0, 1)).foreach(println)
//  send(Array[Byte](0, 0, 0, 2)).foreach(println)
//  send(Array[Byte](0, 0, 0, 3)).foreach(println)


}